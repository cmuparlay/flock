/* 
 * File:   errors.h
 * Author: trbot
 *
 * Created on April 20, 2017, 1:09 PM
 */

#ifndef ERRORS_H
#define	ERRORS_H

#include <iostream>
#include <string>
#include <unistd.h>

#ifndef error
#define setbench_error(s) { \
    std::cout<<"ERROR: "<<s<<" (at "<<__FILE__<<"::"<<__FUNCTION__<<":"<<__LINE__<<")"<<std::endl; \
    exit(-1); \
}
#endif

#endif	/* ERRORS_H */

