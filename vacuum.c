/*
 *     Copyright 2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifdef WIN32
#include <Windows.h>
#include <direct.h>
#include <io.h>
#include "getopt.h"
#define sleep(sec) Sleep(sec * 1000)
#define chdir(dir) _chdir(dir)

#else
#include <dirent.h>
#include <unistd.h>
#include <getopt.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <assert.h>
#include <libcouchbase/couchbase.h>
#include "cJSON.h"

/* The default location we'll monitor for files to upload to Couchbase */
#ifdef WIN32
static const char *default_spool = "C:\\vacuum";
#else
static const char *default_spool = "/var/spool/vacuum";
#endif

/* The instance we're using for all our communication to Couchbase */
lcb_t instance;

/**
 * We have two different error situations with respect to JSON.
 */
enum json_errors {
   /** The content of the file is not JSON (we couldn't parse it) */
   INVALID_JSON,
   /** The JSON document did not have an "_id" tag */
   UNKNOWN_JSON
};

/**
 * Whenever libcoucbase encounters an error it will call the error
 * callback. The error callback will also be called if a timeout
 * occur, so that we could generate alarms instead of having to
 * populate all of that logic into our callback handlers.
 *
 * In our example we'll ignore all timeouts, but treat any other error
 * fatal and terminate the program.
 *
 * @param instance the instance who threw the error
 * @param error the error condition that just happened
 * @param errinfo possibly more information about the error
 */
static void error_callback(lcb_t instance,
                           lcb_error_t error,
                           const char *errinfo)
{
   /* Ignore timeouts... */
   if (error != LCB_ETIMEDOUT) {
      fprintf(stderr, "\rFATAL ERROR: %s\n",
              lcb_strerror(instance, error));
      if (errinfo && strlen(errinfo) != 0) {
         fprintf(stderr, "\t\"%s\"\n", errinfo);
      }
      exit(EXIT_FAILURE);
   }
}

/**
 * Every time a store operation completes lcb will call the
 * registered storage callback to inform the client application about
 * the result of the operation.
 *
 * In our example we'll just temporarily mark the operation as
 * successful or failed. If it failed we'll print out an error message
 * describing the failure.
 *
 * @param instance the instance who just completed a storage operatoin
 * @param cookie the special cookie the user attached to the request
 * @param operation the operation that just completed
 * @param err the error code indicating the result of the operation
 * @param resp the response object
 */
static void store_callback(lcb_t instance,
                           const void *cookie,
                           lcb_storage_t operation,
                           lcb_error_t err,
                           const lcb_store_resp_t *resp)
{
   int *error = (void*)cookie;
   if (err == LCB_SUCCESS) {
      *error = 0;
   } else {
      *error = 1;
      fprintf(stderr, "Failed to store \"");
      fwrite(resp->v.v0.key, 1, resp->v.v0.nkey, stderr);
      fprintf(stderr, "\": %s\n",
              lcb_strerror(instance, err));
      fflush(stderr);
   }
}

/**
 * We need to load the content of file into memory.
 *
 * @param name the name of the file to load (relative to cwd)
 * @param data where to store the data (OUT)
 * @param sz The number of bytes for the file (OUT)
 *
 * @return -1 for error, 0 otherwise
 */
int loadit(const char *name, char **data, size_t *sz)
{
   size_t offset = 0;
   size_t nr;
   struct stat st;
   FILE *fp;
   char *ptr;

   if (stat(name, &st) == -1) {
      fprintf(stderr, "Failed to get information for %s: %s\n",
              name, strerror(errno));
      return -1;
   }

   *sz = st.st_size;
   ptr = *data = malloc(st.st_size + 1);
   if (ptr == NULL) {
      fprintf(stderr, "Failed to allocate memory");
      return -1;
   }
   ptr[st.st_size] = '\0';

   fp = fopen(name, "rb");
   if (fp == NULL) {
      fprintf(stderr, "Failed to open file %s: %s\n", name,
              strerror(errno));
      free(ptr);
      return -1;
   }

   while ((nr = fread(ptr + offset, 1, st.st_size, fp)) != (lcb_size_t)-1) {
      if (nr == 0) {
         break;
      }
      st.st_size -= nr;
      offset += nr;
   }

   if (nr == -1) {
      free(ptr);
      fprintf(stderr, "Failed to read file %s: %s\n", name, strerror(errno));
   }

   fclose(fp);

   return nr == -1 ? -1 : 0;
}

/**
 * If we for some reason failed to process the file we should rename
 * the file so that we don't have to process it anymore (and let the
 * operator handle the problem)
 *
 * @param reason the reason why we couldn't process the file
 * @param name the name of the file
 */
static void invalid_file(enum json_errors reason, const char *name)
{
   char buffer[1024];
   const char *prefix;
   switch (reason) {
   case INVALID_JSON:
      fprintf(stderr, "Filed to validate JSON in %s\n", name);
      prefix = ".invalid";
      break;
   case UNKNOWN_JSON:
      fprintf(stderr, "Unknown (no _id) JSON in %s\n", name);
      prefix = ".unknown";
      break;
   default:
      /* Not implemented yet */
      abort();
   }

   sprintf(buffer, "%s-%s", prefix, name);
   if (rename(name, buffer) == -1) {
      fprintf(stderr, "Failed to rename file: %s\n", strerror(errno));
   }
}

/**
 * Write a histogram of the timing info.
 */
static void timings_callback(lcb_t instance, const void *cookie,
                             lcb_timeunit_t timeunit,
                             lcb_uint32_t min, lcb_uint32_t max,
                             lcb_uint32_t total, lcb_uint32_t maxtotal)
{
   char buffer[1024];
   int num, ii, offset = sprintf(buffer, "[%3u - %3u]", min, max);

   switch (timeunit) {
   case LCB_TIMEUNIT_NSEC:
      offset += sprintf(buffer + offset, "ns");
      break;
   case LCB_TIMEUNIT_USEC:
      offset += sprintf(buffer + offset, "us");
      break;
   case LCB_TIMEUNIT_MSEC:
      offset += sprintf(buffer + offset, "ms");
      break;
   case LCB_TIMEUNIT_SEC:
      offset += sprintf(buffer + offset, "s");
      break;
   default:
      ;
   }

   num = (int)((float)40.0 * (float)total / (float)maxtotal);
   offset += sprintf(buffer + offset, " |");
   for (ii = 0; ii < num; ++ii) {
      offset += sprintf(buffer + offset, "#");
   }

   offset += sprintf(buffer + offset, " - %u\n", total);
   fputs(buffer, (FILE*)cookie);
}

static void process_file(const char *fname)
{
   char *ptr = NULL;
   size_t size;
   cJSON *json, *id;
   lcb_error_t ret;
   int error = 0;
   lcb_store_cmd_t cmd;
   const lcb_store_cmd_t* const cmds[1] = { &cmd };

   if (fname[0] == '.') {
      if (strcmp(fname, ".dump_stats") == 0) {
         fprintf(stdout, "Dumping stats:\n");
         lcb_get_timings(instance, stdout, timings_callback);
         fprintf(stdout, "----\n");
         remove(fname);
      }
      return;
   }

   if (loadit(fname, &ptr, &size) == -1) {
      /* Error message already printed */
      return;
   }

   if ((json = cJSON_Parse(ptr)) == NULL) {
      invalid_file(INVALID_JSON, fname);
      free(ptr);
      return;
   }

   id = cJSON_GetObjectItem(json, "_id");
   if (id == NULL || id->type != cJSON_String) {
      invalid_file(UNKNOWN_JSON, fname);
      free(ptr);
      return;
   }

   memset(&cmd, 0, sizeof(cmd));
   cmd.v.v0.key = id->valuestring;
   cmd.v.v0.nkey = strlen(id->valuestring);
   cmd.v.v0.bytes = ptr;
   cmd.v.v0.nbytes = size;
   cmd.v.v0.operation = LCB_SET;

   ret = lcb_store(instance, &error, 1, cmds);
   if (ret == LCB_SUCCESS) {
      lcb_wait(instance);
   } else {
      error = 1;
   }

   free(ptr);
   if (error) {
      fprintf(stderr, "Failed to store %s: %s\n", fname,
              lcb_strerror(instance, ret));
   } else {
      remove(fname);
   }
}

/**
 * Iterate through all of the files in the directory and try to upload
 * all of them to Couchbase. To be able to "safe" and run lockless we
 * should ignore all files that starts with a dot. The user may then
 * write the file as ".file-to-add" before it is atomically renamed to
 * so something that doesn't start with a dot and we can process it
 * and know that it won't change while we're operating on the file.
 *
 * There is however one "magic" file: ".dump_stats". Creation of this
 * file will make lcb dump it's internal timing statistics.
 */
static void process_directory(void)
{
#ifdef WIN32
   struct _finddata_t file;
   intptr_t handle = _findfirst("*", &file);
   if (handle == -1) {
      fprintf(stderr, "Failed to open directory\n");
      return;
   }

   do {
      process_file(file.name);
   } while (_findnext(handle, &file) != -1);
   _findclose(handle);
#else
   DIR *dp;
   struct dirent *de;

   if ((dp = opendir(".")) == NULL) {
      fprintf(stderr, "Failed to open directory\n");
      return;
   }

   while ((de = readdir(dp)) != NULL) {
      process_file(de->d_name);
   }

   closedir(dp);
#endif
}

static void setup_options(struct option *opts)
{
   opts[0].name = (char*)"spool";
   opts[0].has_arg = required_argument;
   opts[0].val = 's';
   opts[1].name = (char*)"host";
   opts[1].has_arg = required_argument;
   opts[1].val = 'h';
   opts[2].name = (char*)"user";
   opts[2].has_arg = required_argument;
   opts[2].val = 'u';
   opts[3].name = (char*)"password";
   opts[3].has_arg = required_argument;
   opts[3].val = 'p';
   opts[4].name = (char*)"bucket";
   opts[4].has_arg = required_argument;
   opts[4].val = 'b';
   opts[5].name = (char*)"sleep-time";
   opts[5].has_arg = required_argument;
   opts[5].val = 't';
   opts[6].name = NULL;
   opts[6].has_arg = required_argument;
   opts[6].val = -1;
}

/**
 * Program entry point.
 *
 * "monitor" a directory and upload all of the JSON files in that
 * directory to a couchbase server.
 *
 * @param argc argument count
 * @param argv argument vector
 */
int main(int argc, char **argv)
{
   const char *spool = default_spool;
   const char *host = NULL;
   const char *user = NULL;
   const char *passwd = NULL;
   const char *bucket = NULL;
   int sleep_time = 5;
   lcb_error_t ret;
   int cmd;
   struct option opts[7];
   struct lcb_create_st create_options;


   memset(opts, 0, sizeof(opts));
   setup_options(opts);

   /* Parse command line arguments */
   while ((cmd = getopt_long(argc, argv,
                             "s:" /* spool directory */
                             "h:" /* host */
                             "u:" /* user */
                             "p:" /* password */
                             "b:" /* bucket */
                             "t:", /* Sleep time */
                             opts, NULL)) != -1) {
      switch (cmd) {
      case 's' : spool = optarg; break;
      case 'h' : host = optarg; break;
      case 'u' : user = optarg; break;
      case 'p' : passwd = optarg; break;
      case 'b' : bucket = optarg; break;
      case 't' : sleep_time = atoi(optarg); break;
      default:
         fprintf(stderr, "Usage: vacuum [options]\n"
                 "\t-h host\tHostname to connect to (default: localhost:8091)\n"
                 "\t-u user\tUsername to log in with (default: none)\n"
                 "\t-p passwd\tPassword to log in with (default: none)\n"
                 "\t-b name\tName of bucket to connect to (default: default)\n"
                 "\t-v\tEnable verbosity\n"
                 "\t-t sec\tNumber of seconds between each scan (default: 5)\n"
                 "\t-s dir\tLocation of spool directory (default: %s)\n",
                 default_spool);
         exit(EXIT_FAILURE);
      }
   }

   /* Change working directory to the spool directory */
   if (chdir(spool) != 0) {
      fprintf(stderr, "Failed to enter directory %s: %s\n", spool,
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   memset(&create_options, 0, sizeof(create_options));
   create_options.v.v0.host = host;
   create_options.v.v0.user = user;
   create_options.v.v0.passwd = passwd;
   create_options.v.v0.bucket = bucket;

   /* Create the instance to lcb */
   ret = lcb_create(&instance, &create_options);
   if (ret != LCB_SUCCESS) {
      fprintf(stderr, "Failed to create couchbase instance\n");
      exit(EXIT_FAILURE);
   }

   /* Set up the callbacks we want */
   (void)lcb_set_store_callback(instance, store_callback);
   (void)lcb_set_error_callback(instance, error_callback);

   /* Tell libcouchback to connect to the server */
   ret = lcb_connect(instance);
   if (ret != LCB_SUCCESS) {
      fprintf(stderr, "Failed to connect: %s\n",
              lcb_strerror(instance, ret));
      exit(EXIT_FAILURE);
   }

   /* Wait for the server to complete */
   lcb_wait(instance);
   if ((ret = lcb_enable_timings(instance) != LCB_SUCCESS)) {
      fprintf(stderr, "Failed to enable timings: %s\n",
              lcb_strerror(instance, ret));
   }

   /* Loop forever and process the spool directory */
   while (1) {
      int ii;
      int nsec = sleep_time;
      process_directory();
      do {
         fprintf(stdout, "\rsleeping %d secs before retry..", nsec);
         fflush(stdout);
         sleep(1);
         --nsec;
      } while (nsec > 0);
      fprintf(stdout, "\r");
      for (ii = 0; ii < 70; ++ii) {
         fprintf(stdout, " ");
      }
      fprintf(stdout, "\r");
      fflush(stdout);
   }

   return 0;
}
