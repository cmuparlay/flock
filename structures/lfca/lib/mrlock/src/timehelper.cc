#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sstream>
#include <iomanip>
#include <cstdio>
#include "timehelper.h"
#include "config.h"

////////////////////////////////////////////////////////////////////////////////
// Time
double Time::GetWallTime()
{
    double wallTime = 0.0;
    
#ifdef HAVE_CLOCK_GETTIME
    timespec tspec;
    clock_gettime(CLOCK_MONOTONIC, &tspec);
    wallTime += static_cast<double>(tspec.tv_sec);
    wallTime += static_cast<double>(tspec.tv_nsec) * 1e-9;
#elif defined HAVE_GETTIMEOFDAY
    timeval tod;
    gettimeofday(&tod,0);
    wallTime += static_cast<double>(tod.tv_sec);
    wallTime += static_cast<double>(tod.tv_usec) * 1e-6;
#endif // HAVE_CLOCK_GETTIME

    return wallTime;
}

double Time::GetCpuTime()
{
    double cpuTime = 0.0;

#ifdef HAVE_CLOCK_GETTIME
    timespec tspec;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tspec);
    return static_cast<double>(tspec.tv_sec) + static_cast<double>(tspec.tv_nsec) * 1e-9;
#elif defined GETRUSAGE
  rusage r;
  getrusage(RUSAGE_SELF,&r);
  cpuTime += static_cast<double>(r.ru_utime.tv_sec + r.ru_stime.tv_sec);
  cpuTime += static_cast<double>( r.ru_utime.tv_usec + r.ru_stime.tv_usec ) * 1e-6;
#endif // HAVE_GETRUSAGE

  return cpuTime;
}

std::string Time::ToString(double time)
{
    int hours = time / 3600;
    time -= hours * 3600;
    int minutes = time / 60;
    time -= minutes * 60;
    double seconds = time;

    std::stringstream timeStream;

    if (hours)
    {
        timeStream << hours << 'h';
    }

    if (hours || minutes)
    {
        timeStream << minutes << 'm';
    }

    timeStream << std::setprecision(2) << seconds << 's';

    return timeStream.str();
}

std::string Time::ToSecond(double time)
{
    double seconds = time;

    std::stringstream timeStream;

    timeStream << std::setprecision(5) << seconds << 's';

    return timeStream.str();
}
////////////////////////////////////////////////////////////////////////////////
// Timer
Timer::Timer()
: m_cpuStart(0.0)
, m_wallStart(0.0)
, m_cpuElapse(0.0)
, m_wallElapse(0.0)
, m_stopped(true)
{}

Timer::~Timer()
{}

void Timer::Start()
{
    m_cpuStart = Time::GetCpuTime();
    m_wallStart = Time::GetWallTime();
    m_cpuElapse = 0.0;
    m_wallElapse = 0.0;
    m_stopped = false;
}

void Timer::Stop()
{
    m_cpuElapse = Time::GetCpuTime() - m_cpuStart;
    m_wallElapse = Time::GetWallTime() - m_wallStart;
    m_cpuStart = 0.0;
    m_wallStart = 0.0;
    m_stopped = true;
}

void Timer::Resume()
{
    m_cpuStart = Time::GetCpuTime();
    m_wallStart = Time::GetWallTime();
    m_stopped = false;
}

double Timer::ElapsedCpu() const
{
    return m_stopped ? m_cpuElapse : m_cpuElapse + Time::GetCpuTime() - m_cpuStart; 
}

double Timer::ElapsedWall() const
{
    return m_stopped ? m_wallElapse : m_wallElapse + Time::GetWallTime() - m_wallStart; 
}

std::string Timer::ToString() const
{
    std::stringstream timerStream;
    timerStream << "CPU Time: " << Time::ToString(ElapsedCpu()) << " Wall Time: " << Time::ToString(ElapsedWall());

    return timerStream.str();
}

std::string Timer::ToSecond() const
{
    std::stringstream timerStream;
    timerStream << "CPU Time: " << Time::ToSecond(ElapsedCpu()) << " Wall Time: " << Time::ToSecond(ElapsedWall());

    return timerStream.str();
}

////////////////////////////////////////////////////////////////////////////////
// ScopedTimer
ScopedTimer::ScopedTimer(bool showSec)
    : m_showSec(showSec)
{
    Start();
}

ScopedTimer::ScopedTimer(const std::string& tag, bool showSec)
: m_showSec(showSec)
, m_tag(tag)
{
    Start();
}

ScopedTimer::~ScopedTimer()
{
    Stop();

    printf("%s%s\n", m_tag.c_str(), m_showSec ? ToSecond().c_str() : ToString().c_str());
}
