/*
 * Janus RTMP Plugin - broadcasts publisher's streams to external RTMP services.
 * Copyright (C) 2020 Shakuro (https://shakuro.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Author: Aleksey Gureev <agureiev@shakuro.com>
 */

#include "janus/plugin.h"
#include "janus/record.h"
#include "janus/utils.h"
#include "janus/config.h"

#include <errno.h>
#include <gst/gst.h>

/* Plugin information */
#define PLUGIN_RTMP_VERSION         1
#define PLUGIN_RTMP_VERSION_STRING  "0.0.1"
#define PLUGIN_RTMP_DESCRIPTION     "This is an RTMP streaming plugin for Janus, allowing WebRTC peers to send their media to RTMP servers via Gstreamer."
#define PLUGIN_RTMP_NAME            "Janus RTMP plugin"
#define PLUGIN_RTMP_AUTHOR          "agureiev@shakuro.com"
#define PLUGIN_RTMP_PACKAGE         "janus.plugin.rtmp"

#define RTP_RANGE_MIN_DEFAULT 10000
#define RTP_RANGE_MAX_DEFAULT 49999

/* Plugin methods */
janus_plugin *create(void);
int plugin_rtmp_init(janus_callbacks *callback, const char *config_path);
void plugin_rtmp_destroy(void);
int plugin_rtmp_get_api_compatibility(void);
int plugin_rtmp_get_version(void);
const char *plugin_rtmp_get_version_string(void);
const char *plugin_rtmp_get_description(void);
const char *plugin_rtmp_get_name(void);
const char *plugin_rtmp_get_author(void);
const char *plugin_rtmp_get_package(void);
void plugin_rtmp_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *plugin_rtmp_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void plugin_rtmp_setup_media(janus_plugin_session *handle);
void plugin_rtmp_hangup_media(janus_plugin_session *handle);
void plugin_rtmp_destroy_session(janus_plugin_session *handle, int *error);
json_t *plugin_rtmp_query_session(janus_plugin_session *handle);



static janus_plugin janus_rtmp_plugin =
  JANUS_PLUGIN_INIT (
    .init = plugin_rtmp_init,
    .destroy = plugin_rtmp_destroy,

    .get_api_compatibility = plugin_rtmp_get_api_compatibility,
    .get_version = plugin_rtmp_get_version,
    .get_version_string = plugin_rtmp_get_version_string,
    .get_description = plugin_rtmp_get_description,
    .get_name = plugin_rtmp_get_name,
    .get_author = plugin_rtmp_get_author,
    .get_package = plugin_rtmp_get_package,

    .create_session = plugin_rtmp_create_session,
    .handle_message = plugin_rtmp_handle_message,
    .setup_media = plugin_rtmp_setup_media,
    .hangup_media = plugin_rtmp_hangup_media,
    .destroy_session = plugin_rtmp_destroy_session,
    .query_session = plugin_rtmp_query_session,
  );

static volatile gint initialized = 0, stopping = 0;

static volatile gint next_port = RTP_RANGE_MIN_DEFAULT;
static uint16_t rtp_range_min  = RTP_RANGE_MIN_DEFAULT;
static uint16_t rtp_range_max  = RTP_RANGE_MAX_DEFAULT;

#define RTP_RANGE_SIZE rtp_range_max - rtp_range_min + 1 /* inclusive range */

/* Plugin creator */
janus_plugin *create(void) {
  JANUS_LOG(LOG_VERB, "%s created!\n", PLUGIN_RTMP_NAME);
  return &janus_rtmp_plugin;
}

/* Parameter validation */
static struct janus_json_parameter plugin_rtmp_request_parameters[] = {
  {"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter plugin_rtmp_start_live[] = {
  {"url", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};


/** Sessions */
typedef struct plugin_rtmp_session {
  janus_plugin_session *handle;

  gboolean started;
  GstElement *pipeline;
  GstBus *bus;

  uint16_t audio_port;
  uint16_t video_port;

  janus_mutex mutex;
} plugin_rtmp_session;

typedef struct plugin_rtmp_port_mapping {
  plugin_rtmp_session *session;

  uint16_t audio_port;
  uint16_t video_port;
} plugin_rtmp_port_mapping;

static GArray      *ports;
static GHashTable  *sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;


/* Error codes */
#define ERROR_INVALID_REQUEST   411
#define ERROR_INVALID_ELEMENT   412
#define ERROR_MISSING_ELEMENT   413
#define ERROR_UNKNOWN_ERROR     499


/* Internal functions forward declaration. */
static void stop_session_pipeline(plugin_rtmp_session *session);
static gboolean bus_callback(GstBus * bus, GstMessage * message, gpointer data);
static plugin_rtmp_session *session_from_handle(janus_plugin_session *handle);
static gboolean is_valid_url(const char *url);
static GstElement *start_pipeline(const char* url, int audio_port, int video_port, gboolean dummy_audio);
static uint16_t get_free_port(void);
static void set_port_mapping(plugin_rtmp_session *session, uint16_t aport, uint16_t vport);
static void clear_port_mapping(plugin_rtmp_session *session);

/* Message handlers. */
static janus_plugin_result *handle_message(plugin_rtmp_session *session, json_t *root);
static janus_plugin_result *handle_message_start(plugin_rtmp_session *session, json_t* root);
static janus_plugin_result *handle_message_stop(plugin_rtmp_session *session, json_t *root);


// ------------------------------------------------------------------------------------------------
// Plugin implementation
// ------------------------------------------------------------------------------------------------

int plugin_rtmp_init(janus_callbacks *callback, const char *config_path) {
	JANUS_LOG(LOG_INFO, "%s initialized!\n", PLUGIN_RTMP_NAME);
	sessions = g_hash_table_new(NULL, NULL);
	if (!sessions) {
		JANUS_LOG(LOG_ERR, "Failed to allocate sessions table\n");
		return -1;
	}

	ports = g_array_new(FALSE, TRUE, sizeof(plugin_rtmp_port_mapping));
	if (!ports) {
		JANUS_LOG(LOG_ERR, "Failed to allocate ports table\n");
		return -1;
	}

	/* Read configuration */
	if (config_path != NULL) {
		char filename[BUFSIZ];
		g_snprintf(filename, BUFSIZ, "%s/%s.jcfg", config_path, PLUGIN_RTMP_PACKAGE);
		JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);

		janus_config *config = janus_config_parse(filename);
		if (config == NULL) {
			JANUS_LOG(LOG_WARN, "Couldn't find .jcfg configuration file (%s), trying .cfg\n", PLUGIN_RTMP_PACKAGE);
			g_snprintf(filename, BUFSIZ, "%s/%s.cfg", config_path, PLUGIN_RTMP_PACKAGE);

			JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
			config = janus_config_parse(filename);
		}

		if (config != NULL) {
			janus_config_print(config);

			janus_config_category *config_general = janus_config_get_create(config, NULL, janus_config_type_category, "general");
			janus_config_item     *item           = janus_config_get(config, config_general, janus_config_type_item, "rtp_port_range");
			if (item && item->value) {
				/* Split in min and max port */

				char *maxport = strrchr(item->value, '-');
				if (maxport != NULL) {
					*maxport = '\0';
					maxport++;

					if (janus_string_to_uint16(item->value, &rtp_range_min) < 0)
						JANUS_LOG(LOG_WARN, "Invalid RTP min port value: %s (assuming 0)\n", item->value);

					if (janus_string_to_uint16(maxport, &rtp_range_max) < 0)
						JANUS_LOG(LOG_WARN, "Invalid RTP max port value: %s (assuming 0)\n", maxport);

					maxport--;
					*maxport = '-';
				}

				if (rtp_range_min > rtp_range_max) {
					uint16_t temp_port = rtp_range_min;
					rtp_range_min = rtp_range_max;
					rtp_range_max = temp_port;
				}

				if (rtp_range_max == 0)
					rtp_range_max = 65535;

				next_port = rtp_range_min;

				if (RTP_RANGE_SIZE) {
					plugin_rtmp_port_mapping template = {
						.session = NULL,
						.audio_port = 0,
						.video_port = 0
					};

					uint16_t i = 0;
					for (;i < RTP_RANGE_SIZE; i++) {
						g_array_append_val(ports, template);
					}

					JANUS_LOG(LOG_VERB, "Ports mapping array size: %u\n", ports->len);
				}

				JANUS_LOG(LOG_VERB, "Gstreamer RTMP port range: %u -- %u\n", rtp_range_min, rtp_range_max);
			}

			janus_config_destroy(config);
			config = NULL;
		}
	} else {
		JANUS_LOG(LOG_WARN, "No config_path provided\n");
	}

	g_atomic_int_set(&initialized, 1);
	gst_init(NULL, NULL);

	return 0;
}

void plugin_rtmp_destroy(void) {
  JANUS_LOG(LOG_INFO, "%s destroyed!\n", PLUGIN_RTMP_NAME);
  g_hash_table_destroy(sessions);

  gst_deinit();
}

int plugin_rtmp_get_api_compatibility(void) {
  return JANUS_PLUGIN_API_VERSION;
}

int plugin_rtmp_get_version(void) {
  return PLUGIN_RTMP_VERSION;
}

const char *plugin_rtmp_get_version_string(void) {
  return PLUGIN_RTMP_VERSION_STRING;
}

const char *plugin_rtmp_get_description(void) {
  return PLUGIN_RTMP_DESCRIPTION;
}

const char *plugin_rtmp_get_name(void) {
  return PLUGIN_RTMP_NAME;
}

const char *plugin_rtmp_get_author(void) {
  return PLUGIN_RTMP_AUTHOR;
}

const char *plugin_rtmp_get_package(void) {
  return PLUGIN_RTMP_PACKAGE;
}

static plugin_rtmp_session *plugin_rtmp_lookup_session(janus_plugin_session *handle) {
  plugin_rtmp_session *session = NULL;
  if (g_hash_table_contains(sessions, handle)) {
    session = (plugin_rtmp_session *)handle->plugin_handle;
  }
  return session;
}

void plugin_rtmp_create_session(janus_plugin_session *handle, int *error) {
  if (g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
    *error = -1;
    return;
  }

  plugin_rtmp_session *session = g_malloc0(sizeof(plugin_rtmp_session));
  session->handle = handle;
  session->started = FALSE;
  session->pipeline = NULL;
  handle->plugin_handle = session;

  janus_mutex_lock(&sessions_mutex);
  g_hash_table_insert(sessions, handle, session);
  janus_mutex_unlock(&sessions_mutex);

  return;
}

void plugin_rtmp_destroy_session(janus_plugin_session *handle, int *error) {
  if (g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
    *error = -1;
    return;
  }

  janus_mutex_lock(&sessions_mutex);

  plugin_rtmp_session *session = plugin_rtmp_lookup_session(handle);
  if (!session) {
    JANUS_LOG(LOG_ERR, "No Live session associated with this handle...\n");
    *error = -2;
  } else {
    JANUS_LOG(LOG_VERB, "Removing Live session...\n");
    stop_session_pipeline(session);
    g_hash_table_remove(sessions, handle);
  }

  janus_mutex_unlock(&sessions_mutex);
  return;
}

json_t *plugin_rtmp_query_session(janus_plugin_session *handle) {
  json_t *result;

  if (!g_atomic_int_get(&stopping) && g_atomic_int_get(&initialized) && session_from_handle(handle) != NULL) {
    result = json_object();
  }

  return result;
}

struct janus_plugin_result *plugin_rtmp_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
  janus_plugin_result *result;

  // Check we aren't stopping and initialized
  if (g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
    result = janus_plugin_result_new(JANUS_PLUGIN_ERROR, g_atomic_int_get(&stopping) ? "Shutting down" : "Plugin not initialized", NULL);
  } else {
    plugin_rtmp_session *session = session_from_handle(handle);

    if (session) {
      result = handle_message(session, message);
    } else {
      JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
      result = janus_plugin_result_new(JANUS_PLUGIN_ERROR, "No session associated with this handle", NULL);
    }
  }

  // Release resources
  if (message != NULL) json_decref(message);
  if (jsep != NULL) json_decref(jsep);
  g_free(transaction);

  return result;
}

void plugin_rtmp_setup_media(janus_plugin_session *handle) {
  JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC media is now available\n", PLUGIN_RTMP_PACKAGE, handle);
}

void plugin_rtmp_hangup_media(janus_plugin_session *handle) {
  JANUS_LOG(LOG_INFO, "[%s-%p] No WebRTC media anymore\n", PLUGIN_RTMP_PACKAGE, handle);
  janus_mutex_lock(&sessions_mutex);
  plugin_rtmp_session *session = plugin_rtmp_lookup_session(handle);
  stop_session_pipeline(session);
  janus_mutex_unlock(&sessions_mutex);
}

// ------------------------------------------------------------------------------------------------
// Message handlers
// ------------------------------------------------------------------------------------------------

static janus_plugin_result *handle_message(plugin_rtmp_session *session, json_t *root) {
  janus_plugin_result *result;

  /* Increase the reference counter for this session: we'll decrease it after we handle the message */
  if (!root) return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "No message", NULL);
  if (!json_is_object(root)) return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "JSON error: not an object", NULL);

  /* Pre-parse the message */
  int error_code = 0;
  char error_cause[512];

  /* Get the request first */
  JANUS_VALIDATE_JSON_OBJECT(root, plugin_rtmp_request_parameters,
    error_code, error_cause, TRUE,
    ERROR_MISSING_ELEMENT, ERROR_INVALID_ELEMENT);
  // TODO: do something with error_code / error_cause from parser

  json_t *request = json_object_get(root, "request");
  const char *request_text = json_string_value(request);

  if (!strcasecmp(request_text, "start")) {
    result = handle_message_start(session, root);
  } else if (!strcasecmp(request_text, "stop")) {
    result = handle_message_stop(session, root);
  }

  if (!result) {
    JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
    result = janus_plugin_result_new(JANUS_PLUGIN_ERROR, "Unknown request", NULL);
  }

  return result;
}

static janus_plugin_result *handle_message_start(plugin_rtmp_session *session, json_t* root) {
  JANUS_LOG(LOG_INFO, "[%s] Handling start\n", PLUGIN_RTMP_PACKAGE);

  uint16_t audio_port = 0;
  uint16_t video_port = 0;
  char      url[BUFSIZ] = {0};
  gboolean  dummy_audio = FALSE;

  json_t *url_value = json_object_get(root, "url");
  if (json_string_value(url_value)) {
    snprintf(url, sizeof(url), "%s", json_string_value(url_value));
  }
  if (url_value != NULL) {
    json_decref(url_value);
  }

  json_t *dummy_audio_value  = json_object_get(root, "dummyAudio");
  dummy_audio = json_boolean_value(dummy_audio_value);
  if (dummy_audio_value != NULL) {
    json_decref(dummy_audio_value);
  }

  if (!is_valid_url(url)) {
    return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "Invalid URL format", NULL);
  }

  janus_mutex_lock(&session->mutex);

  audio_port = get_free_port();
  video_port = get_free_port();

  if (audio_port && video_port) {
    JANUS_LOG(LOG_INFO, "[%s] Session %p got aport: %u, vport: %u\n", PLUGIN_RTMP_PACKAGE, session, audio_port, video_port);

    set_port_mapping(session, audio_port, video_port);
    session->audio_port = audio_port;
    session->video_port = video_port;
  } else {
    janus_mutex_unlock(&session->mutex);
    return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "No more ports available", NULL);
  }

  GstElement *pipeline = start_pipeline(url, audio_port, video_port, dummy_audio);

  // Start watching the pipeline bus for events
  GstBus *bus = gst_element_get_bus(pipeline);
  guint _bus_watch_id = gst_bus_add_watch(bus, bus_callback, NULL);

  session->pipeline = pipeline;
  session->bus = bus;

  janus_mutex_unlock(&session->mutex);

  json_t *response = json_object();
  json_object_set_new(response, "streaming", json_string("started"));
  json_object_set_new(response, "audio_port", json_integer((int)audio_port));
  json_object_set_new(response, "video_port", json_integer((int)video_port));

  return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, response);
}


static janus_plugin_result *handle_message_stop(plugin_rtmp_session *session, json_t *root) {
  JANUS_LOG(LOG_INFO, "[%s] Handling stop\n", PLUGIN_RTMP_PACKAGE);

  if (session->pipeline == NULL) {
    return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "Live streaming hasn't been started", NULL);
  }

  stop_session_pipeline(session);

  json_t *response = json_object();
  json_object_set_new(response, "streaming", json_string("stopped"));

  return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, response);
}


static void stop_session_pipeline(plugin_rtmp_session *session) {
  janus_mutex_lock(&session->mutex);

  if (session->pipeline) {
    gst_element_send_event(session->pipeline, gst_event_new_eos());
    gst_element_set_state(session->pipeline, GST_STATE_NULL);
    gst_object_unref(GST_OBJECT(session->pipeline));
    session->pipeline = NULL;
  }

  if (session->bus) {
    gst_object_unref(GST_OBJECT(session->bus));
    session->bus = NULL;
  }

  clear_port_mapping(session);

  janus_mutex_unlock(&session->mutex);
}

/* use under the session lock */
static uint16_t get_free_port(void)
{
	uint16_t port = 0;
	uint16_t counter = 0;

	while (counter < RTP_RANGE_SIZE) {
		plugin_rtmp_port_mapping *map = NULL;

		if (next_port <= rtp_range_max) {
			port = next_port++;
		} else {
			next_port = rtp_range_min;
			port = next_port++;
		}

		map = &g_array_index(ports, plugin_rtmp_port_mapping, port - rtp_range_min);
		if (map && !map->session)
			return port;
		if (map && map->session)
			JANUS_LOG(LOG_HUGE, "[%s] Port %u is used by session %p, skipping\n", PLUGIN_RTMP_PACKAGE, port, map->session );

		counter++;
	}

	JANUS_LOG(LOG_ERR, "[%s] no free ports found\n", PLUGIN_RTMP_PACKAGE);

	return 0;
}

/* use under the session lock */
static void set_port_mapping(plugin_rtmp_session *session, uint16_t aport, uint16_t vport)
{
	plugin_rtmp_port_mapping *amap = NULL;
	plugin_rtmp_port_mapping *vmap = NULL;

	amap = &g_array_index(ports, plugin_rtmp_port_mapping, aport);
	amap->session    = session;
	amap->audio_port = aport;
	amap->video_port = vport;

	vmap = &g_array_index(ports, plugin_rtmp_port_mapping, vport);
	vmap->session    = session;
	vmap->audio_port = aport;
	vmap->video_port = vport;
}

/* use under the session lock */
static void clear_port_mapping(plugin_rtmp_session *session)
{
	plugin_rtmp_port_mapping *amap = NULL;
	plugin_rtmp_port_mapping *vmap = NULL;

	if (session)
	amap = &g_array_index(ports, plugin_rtmp_port_mapping, session->audio_port);
	amap->session    = NULL;
	amap->audio_port = 0;
	amap->video_port = 0;

	vmap = &g_array_index(ports, plugin_rtmp_port_mapping, session->video_port);
	vmap->session    = NULL;
	vmap->audio_port = 0;
	vmap->video_port = 0;
}

// ------------------------------------------------------------------------------------------------
// Utility
// ------------------------------------------------------------------------------------------------

static plugin_rtmp_session *session_from_handle(janus_plugin_session *handle) {
  plugin_rtmp_session *session;

  janus_mutex_lock(&sessions_mutex);
  session = plugin_rtmp_lookup_session(handle);
  janus_mutex_unlock(&sessions_mutex);

  return session;
}

// Validates it's RTMP(S) URL.
static gboolean is_valid_url(const char *url) {
  if (!url)
    return 0;

  return g_regex_match_simple("^rtmps?://.+", url, 0, 0);
}

// ------------------------------------------------------------------------------------------------
// Pipeline
// ------------------------------------------------------------------------------------------------

static GstElement *create_pipeline(const char *url, int audio_port, int video_port, gboolean dummy_audio) {
  GstElement *pipeline;
  gchar *pipeline_def = NULL;

  if (dummy_audio) {
    pipeline_def = g_strdup_printf(
      "rtpbin name=rtpbin "
      "udpsrc address=127.0.0.1 port=%d caps=\"application/x-rtp, media=audio, encoding-name=OPUS, clock-rate=48000\" ! rtpbin.recv_rtp_sink_1 "
      "udpsrc address=127.0.0.1 port=%d caps=\"application/x-rtp, media=video, encoding-name=H264, clock-rate=90000\" ! rtpbin.recv_rtp_sink_0 "
      "rtpbin. ! rtph264depay ! flvmux streamable=true name=mux ! rtmpsink location=\"%s\" "
      "audiotestsrc wave=silence ! voaacenc ! aacparse ! mux.",
      audio_port, video_port, url);
  } else {
    pipeline_def = g_strdup_printf(
        "rtpbin name=rtpbin "
        "udpsrc address=127.0.0.1 port=%d caps=\"application/x-rtp, media=audio, encoding-name=OPUS, clock-rate=48000\" ! rtpbin.recv_rtp_sink_1 "
        "udpsrc address=127.0.0.1 port=%d caps=\"application/x-rtp, media=video, encoding-name=H264, clock-rate=90000\" ! rtpbin.recv_rtp_sink_0 "
        "rtpbin. ! rtph264depay ! flvmux streamable=true name=mux ! rtmpsink location=\"%s\" "
        "rtpbin. ! rtpopusdepay ! queue ! opusdec ! audioconvert ! audioresample ! voaacenc bitrate=128000 ! aacparse ! mux.",
        audio_port, video_port, url);
  }

  JANUS_LOG(LOG_INFO, "Pipeline definition: %s\n", pipeline_def);

  pipeline = gst_parse_launch(pipeline_def, NULL);

  g_free(pipeline_def);

  return pipeline;
}


static GstElement* start_pipeline(const char* url, int audio_port, int video_port, gboolean dummy_audio) {
  GstElement *pipeline = create_pipeline(url, audio_port, video_port, dummy_audio);

  if (!pipeline) {
    JANUS_LOG(LOG_ERR, "Could not create the pipeline");
    return NULL;
  }

  JANUS_LOG(LOG_INFO, "Pipeline started (ports audio: %d video: %d)\n", audio_port, video_port);

  // Start playing
  gst_element_set_state(pipeline, GST_STATE_PLAYING);

  return pipeline;
}

static gboolean bus_callback(GstBus *bus, GstMessage *message, gpointer data) {
  GError *err;
  gchar *debug;

  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:
      gst_message_parse_error (message, &err, &debug);
      g_print("Error: %s\n", err->message);
      g_error_free (err);
      g_free (debug);
      break;

    case GST_MESSAGE_EOS:
      g_print("End of stream");
      break;

    case GST_MESSAGE_STATE_CHANGED: {
      GstState old_state, new_state, pending;

      gst_message_parse_state_changed(message, &old_state, &new_state, &pending);
      g_print("Element %s state changed from %s to %s\n", 
        GST_OBJECT_NAME (message->src),
        gst_element_state_get_name (old_state),
        gst_element_state_get_name (new_state));
      break;
    }

    default:
      /* unhandled message */
      g_print("Got %s message\n", GST_MESSAGE_TYPE_NAME (message));
      break;
  }
  return TRUE;
}

