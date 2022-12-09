#!/bin/sh

(glibtoolize || libtoolize)  && autoreconf --force --install
