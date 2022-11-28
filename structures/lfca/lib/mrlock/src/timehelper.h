#ifndef _TIMEHELPER_INCLUDED_
#define _TIMEHELPER_INCLUDED_

#include <string>

////////////////////////////////////////////////////////////////////////////////
// Time
class Time 
{
public:
    static double GetWallTime();
    static double GetCpuTime();

    static std::string ToString(double time);
    static std::string ToSecond(double time);
};

////////////////////////////////////////////////////////////////////////////////
// Timer
class Timer
{
public:
    Timer();
    virtual ~Timer();

    void Start();
    void Stop();
    void Resume();

    double ElapsedCpu() const;
    double ElapsedWall() const;

    std::string ToString() const;
    std::string ToSecond() const;

private:
    double m_cpuStart;
    double m_wallStart;
    double m_cpuElapse;
    double m_wallElapse;
    bool m_stopped;
};

////////////////////////////////////////////////////////////////////////////////
// ScopedTimer
class ScopedTimer : public Timer
{
public:
    ScopedTimer(bool showSec = false);
    ScopedTimer(const std::string& tag, bool showSec = false);
    virtual ~ScopedTimer();

private:
    bool m_showSec;
    std::string m_tag;
};

#endif //_TIMEHELPER_INCLUDED_

