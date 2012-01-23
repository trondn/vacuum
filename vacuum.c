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
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <assert.h>
#include <unistd.h>
#include <libcouchbase/couchbase.h>

#include "cJSON.h"

/* The default location we'll monitor for files to upload to Couchbase */
static const char *default_spool = "/var/spool/vacuum";

/* The instance we're using for all our communication to Couchbase */
libcouchbase_t instance;

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
static void error_callback(libcouchbase_t instance,
                           libcouchbase_error_t error,
                           const char *errinfo)
{
    /* Ignore timeouts... */
    if (error != LIBCOUCHBASE_ETIMEDOUT) {
        fprintf(stderr, "\rFATAL ERROR: %s\n",
                libcouchbase_strerror(instance, error));
        if (errinfo && strlen(errinfo) != 0) {
            fprintf(stderr, "\t\"%s\"\n", errinfo);
        }
        exit(EXIT_FAILURE);
    }
}

/**
 * Every time a store operation completes libcouchbase will call the
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
 * @param key the key for the operation
 * @param nkey the number of bytes the key consists of
 * @param cas the cas value for the object if the operation succeeded
 */
static void storage_callback(libcouchbase_t instance,
                             const void *cookie,
                             libcouchbase_storage_t operation,
                             libcouchbase_error_t err,
                             const void *key, size_t nkey,
                             uint64_t cas)
{
   int *error = (void*)cookie;
    if (err == LIBCOUCHBASE_SUCCESS) {
        *error = 0;
    } else {
        *error = 1;
        fprintf(stderr, "Failed to store \"");
        fwrite(key, 1, nkey, stderr);
        fprintf(stderr, "\": %s\n",
                libcouchbase_strerror(instance, err));
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
      -1;
   }
   ptr[st.st_size] = '\0';

   fp = fopen(name, "rb");
   if (fp == NULL) {
      fprintf(stderr, "Failed to open file %s: %s\n", name,
              strerror(errno));
      free(ptr);
      return -1;
   }

   size_t offset = 0;
   size_t nr;
   while ((nr = fread(ptr + offset, 1, st.st_size, fp)) != (size_t)-1) {
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

   snprintf(buffer, sizeof(buffer), "%s-%s", prefix, name);
   if (rename(name, buffer) == -1) {
      fprintf(stderr, "Failed to rename file: %s\n", strerror(errno));
   }
}

/**
 * Write a histogram of the timing info.
 */
static void timings_callback(libcouchbase_t instance, const void *cookie,
                            libcouchbase_timeunit_t timeunit,
                            uint32_t min, uint32_t max,
                            uint32_t total, uint32_t maxtotal)
{
   char buffer[1024];
   int offset = sprintf(buffer, "[%3u - %3u]", min, max);
   switch (timeunit) {
   case LIBCOUCHBASE_TIMEUNIT_NSEC:
      offset += sprintf(buffer + offset, "ns");
      break;
   case LIBCOUCHBASE_TIMEUNIT_USEC:
      offset += sprintf(buffer + offset, "us");
      break;
   case LIBCOUCHBASE_TIMEUNIT_MSEC:
      offset += sprintf(buffer + offset, "ms");
      break;
   case LIBCOUCHBASE_TIMEUNIT_SEC:
      offset += sprintf(buffer + offset, "s");
      break;
   default:
      ;
   }

   int num = (float)40.0 * (float)total / (float)maxtotal;
   offset += sprintf(buffer + offset, " |");
   for (int ii = 0; ii < num; ++ii) {
      offset += sprintf(buffer + offset, "#");
   }

   offset += sprintf(buffer + offset, " - %u\n", total);
   fputs(buffer, (FILE*)cookie);
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
 * file will make libcouchbase dump it's internal timing statistics.
 */
static void process_directory(void)
{
   DIR *dp;
   struct dirent *de;

   if ((dp = opendir(".")) == NULL) {
      fprintf(stderr, "Failed to open directory\n");
      return;
   }

   while ((de = readdir(dp)) != NULL) {
      char *ptr = NULL;
      size_t size;
      cJSON *json, *id;
      libcouchbase_error_t ret;
      int error = 0;

      if (de->d_name[0] == '.') {
         if (strcmp(de->d_name, ".dump_stats") == 0) {
            fprintf(stdout, "Dumping stats:\n");
            libcouchbase_get_timings(instance, stdout, timings_callback);
            fprintf(stdout, "----\n");

            remove(de->d_name);
         }
         continue;
      }

      if (loadit(de->d_name, &ptr, &size) == -1) {
         /* Error message already printed */
         continue;
      }

      if ((json = cJSON_Parse(ptr)) == NULL) {
         invalid_file(INVALID_JSON, de->d_name);
         free(ptr);
         continue;
      }

      id = cJSON_GetObjectItem(json, "_id");
      if (id == NULL || id->type != cJSON_String) {
         invalid_file(UNKNOWN_JSON, de->d_name);
         free(ptr);
         continue;
      }

      ret = libcouchbase_store(instance, &error, LIBCOUCHBASE_SET,
                               id->valuestring, strlen(id->valuestring),
                               ptr, size, 0, 0, 0);
      if (ret == LIBCOUCHBASE_SUCCESS) {
         libcouchbase_wait(instance);
      } else {
         error = 1;
      }

      free(ptr);
      if (error) {
         fprintf(stderr, "Failed to store %s: %s\n", de->d_name,
                 libcouchbase_strerror(instance, ret));
      } else {
         remove(de->d_name);
      }
   }

   closedir(dp);
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
void main(int argc, char **argv)
{
   const char *spool = default_spool;
   const char *host = NULL;
   const char *user = NULL;
   const char *passwd = NULL;
   const char *bucket = NULL;
   int sleep_time = 5;

   /* Parse command line arguments */
   int cmd;
   while ((cmd = getopt(argc, argv,
                        "s:" /* spool directory */
                        "h:" /* host */
                        "u:" /* user */
                        "p:" /* password */
                        "b:" /* bucket */
                        "t:" /* Sleep time */
                        )) != -1) {
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

   /* Create the instance to libcouchbase */
   instance = libcouchbase_create(host, user, passwd, bucket, NULL);
   if (instance == NULL) {
      fprintf(stderr, "Failed to create couchbase instance\n");
      exit(EXIT_FAILURE);
   }

   /* Set up the callbacks we want */
   (void)libcouchbase_set_storage_callback(instance, storage_callback);
   (void)libcouchbase_set_error_callback(instance, error_callback);

   /* Tell libcouchback to connect to the server */
   libcouchbase_error_t ret = libcouchbase_connect(instance);
   if (ret != LIBCOUCHBASE_SUCCESS) {
      fprintf(stderr, "Failed to connect: %s\n",
              libcouchbase_strerror(instance, ret));
      exit(EXIT_FAILURE);
   }

   /* Wait for the server to complete */
   libcouchbase_wait(instance);
   if ((ret = libcouchbase_enable_timings(instance) != LIBCOUCHBASE_SUCCESS)) {
      fprintf(stderr, "Failed to enable timings: %s\n",
              libcouchbase_strerror(instance, ret));
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
}
