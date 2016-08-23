#!/bin/bash

java -jar ${CADC_ROOT}/lib/cadcVOS.jar $*
progStatus=$?
exit $progStatus

