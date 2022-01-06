/**
 * Original wrapper for linux perf-tools by Henrik Mühe
 * https://muehe.org/posts/profiling-only-parts-of-your-code-with-perf/
 * 
 * Slightly modified to play well with PAPI, and to allow specifying the pmu event.
 */

#ifndef PERFTOOLS_H
#define PERFTOOLS_H

#include <sstream>
#include <iostream>
#include <functional>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>

struct PerfTools
{
    static void profile(const std::string& name, char * pmu_event, std::function<void()> body) {
        if (pmu_event[0] != '\0') {
            std::string filename = name.find(".data") == std::string::npos ? (name + ".data") : name;

            // Launch profiler
            pid_t pid;
            std::stringstream s;
            s << getpid();
            std::cout<<"forking perf thread to monitor self (pid="<<s.str()<<")"<<std::endl;
            pid = fork();
            std::cout<<"forked (pid="<<pid<<")"<<std::endl;
            if (pid == 0) {
                auto fd=open("/dev/null",O_RDWR);
                dup2(fd,1);
                dup2(fd,2);
                std::cout<<"launching perf"<<std::endl;
                exit(execl("/usr/bin/perf","perf","record","-o",filename.c_str(),"-p",s.str().c_str(),"-e",pmu_event,"-g",nullptr));
            }

            // Run body
            std::cout<<"running perf measured body()"<<std::endl;
            body();

            // Kill profiler  
            std::cout<<"killing perf"<<std::endl;
            kill(pid,SIGINT);
            std::cout<<"waiting on perf"<<std::endl;
            waitpid(pid,nullptr,0);
            std::cout<<"finished waiting"<<std::endl;
        } else {
            body();
        }
    }
};

#endif /* PERFTOOLS_H */

