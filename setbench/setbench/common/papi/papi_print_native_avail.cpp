#include <papi.h>
#include <stdio.h>
#include <stdlib.h>

int main()
{
    // int EventCode, retval;
    // char EventCodeStr[PAPI_MAX_STR_LEN];

    /* Initialize the library */
    auto retval = PAPI_library_init(PAPI_VER_CURRENT);

    if  (retval != PAPI_VER_CURRENT) {
        printf("PAPI library init error!\n");
        exit(1);
    }

    int i = PAPI_L2_DCM;
    if (PAPI_OK != PAPI_enum_event(&i, PAPI_ENUM_FIRST)) {
        printf("error\n");
    }
    do {
        PAPI_event_info_t info;
        retval = PAPI_get_event_info(i, &info);
        if (retval == PAPI_OK) {
            printf("%-30s 0x%-10x %s\n", info.symbol, info.event_code, info.long_descr);
        } else {
            printf("ERROR: %s\n", PAPI_strerror(retval));
        }

    //    /* Translate the integer code to a string */
    //    if (PAPI_event_code_to_name(EventCode, EventCodeStr) == PAPI_OK)

    //       /* Print all the native events for this platform */
    //       printf("Name: %s\nCode: %x\n", EventCodeStr, EventCode);

    } while (PAPI_enum_event(&i, PAPI_PRESET_ENUM_AVAIL) == PAPI_OK);
    return 0;
}
