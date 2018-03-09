#!/usr/bin/env lsc -cj
#

# Known issue:
#   when executing the `package.ls` directly, there is always error
#   "/usr/bin/env: lsc -cj: No such file or directory", that is because `env`
#   doesn't allow space.
#
#   More details are discussed on StackOverflow:
#     http://stackoverflow.com/questions/3306518/cannot-pass-an-argument-to-python-with-usr-bin-env-python
#
#   The alternative solution is to add `envns` script to /usr/bin directory
#   to solve the _no space_ issue.
#
#   Or, you can simply type `lsc -cj package.ls` to generate `package.json`
#   quickly.
#

# package.json
#
name: \tic-legacy-hub

author:
  name: \yagamy
  email: \yagamy@t2t.io

description: 'SensorHub Simple Server for V1 (Legacy) Data Format from TOE devices'

version: \0.0.1

repository:
  type: \git
  url: ''

engines:
  node: \4.4.x

ava:
  files:
    * "tests/*.js"
    * "test/*.js"

scripts:
  test: "dms db delete sandbox && lsc -c ./tests/*.ls && ava --verbose"

dependencies:
  colors: \*
  async: \*
  lodash: \*
  yargs: \*
  express: \*
  multer: \*
  prettyjson: \*
  \moment-timezone : \*
  \bunyan-rotating-file-stream : \*
  \bunyan-debug-stream : \*
  \express-bunyan-logger : \*


devDependencies:
  ava: \^0.24.0

keywords: <[tic sensorhub legacy]>

license: \MIT
