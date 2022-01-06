#!/bin/bash

if [ "$#" -ne "1" ]; then
    echo "USAGE: $(basename $0) <ld_library_name>"
    echo "       for example, if you normally compile/link a library with '-lnuma',"
    echo "       then <ld_library_name> should be 'numa'"
    exit 1
fi
ld_library_name=$1

# if [ -e "Makefile.var" ]; then
#     mv Makefile.var Makefile.var.temp
#     cat Makefile.var.temp | grep -v "has_lib${ld_library_name}=" > Makefile.var
#     rm Makefile.var.temp >/dev/null 2>&1
# fi

echo -e "#include <iostream>\nusing namespace std;\nint main() {\nreturn 0;\n}" > _temp.cpp
g++ _temp.cpp -l${ld_library_name} >/dev/null 2>&1
result=$((1 - $?))
# echo "has_lib${ld_library_name}=$result" >> Makefile.var
echo $result
rm _temp.cpp >/dev/null 2>&1
rm a.out >/dev/null 2>&1
