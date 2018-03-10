#!/usr/bin/env lsc
#
require! <[fs path http zlib]>
require! <[colors yargs express multer prettyjson]>
moment = require \moment-timezone

const ONE_MONTH = 30 * 24 * 60 * 60 * 1000
const ONE_HOUR = 60 * 60 * 1000


class DataItem
  (@profile, @id, @item) ->
    {desc, data} = item
    {board_type, board_id, sensor, data_type} = desc
    {updated_at, value, type, unit_length} = data
    @invalid = yes
    return unless board_type? and \string is typeof board_type
    return unless board_id? and \string is typeof board_id
    return unless sensor? and \string is typeof sensor
    return unless data_type? and \string is typeof data_type
    return unless updated_at? and \string is typeof updated_at
    @now = now = (new Date!) - 0
    updated_at = Date.parse updated_at
    return if updated_at === NaN
    @board_type = board_type
    @board_id = board_id
    @sensor = sensor
    @data_type = data_type
    @updated_at = updated_at
    @time_shift = ts = now - updated_at
    @time_shifts = [ts]
    @type = type
    @invalid = no
    p = "#{board_type}/#{board_id}/#{sensor}/#{data_type}"
    @prefix = "[#{id.yellow}.#{updated_at}] #{p.green}"
    value = parseFloat value.toFixed 2 if \process is board_type and \number is type
    @value = value

  show-message: (message, ret=no) ->
    {verbose} = global.argv
    return unless verbose
    INFO "#{@prefix} => #{message.gray}"
    return ret

  is-broadcastable: ->
    {invalid, board_type, board_id, sensor, data_type, updated_at, now, value, type, time_shift, now} = @
    return @.show-message "invalid data item" if invalid
    return @.show-message "value is NULL" unless value?
    return @.show-message "value is STRING" if \string is typeof value
    return @.show-message "data comes from future (at least one hour later). #{updated_at} v.s. #{now}" if (time_shift + ONE_HOUR) < 0
    return @.show-message "data came from one month ago. #{updated_at} v.s. #{now}" if time_shift > ONE_MONTH
    return yes

  to-array: ->
    {board_type, board_id, sensor, data_type, updated_at, value, time_shifts} = self = @
    return [updated_at, board_type, board_id, sensor, data_type, value, time_shifts]


NG = (message, code, status, req, res) ->
  {configs} = module
  url = req.originalUrl
  result = {url, code, message, configs}
  return res.status status .json result


INFO = (message) ->
  now = moment!
  console.log "#{now.format!} [INFO] #{message}"


ERR = (message) ->
  now = moment!
  console.log "#{now.format!} [ERR ] #{message}"


DUMP_ITEMS = (items) ->
  {dump} = global.argv
  return unless dump
  text = prettyjson.render items, do
    keysColor: \gray
    dashColor: \green
    stringColor: \yellow
    numberColor: \cyan
    defaultIndentation: 4
    inlineArrays: yes
  zs = text.split '\n'
  # console.error "#{profile.yellow}/#{id.green}:"
  [ console.error "\t#{z}" for z in zs ]



PROCESS_COMPRESSED_DATA = (originalname, buffer, id, profile, req, res) ->
  x = "#{buffer.length}"
  INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => receive #{x.magenta} bytes"
  result = bytes: buffer.length, id: id, profile: profile, filename: originalname
  (zerr, raw) <- zlib.gunzip buffer
  if zerr?
    message = "#{profile}/#{id}/#{originalname} decompression failure."
    ERR zerr, message
    # Although the uploaded archive is not a valid gzip file,
    # we still need to accept it. Otherwise, the client will keep uploading
    # the corrupted archive file onto sensor-hub.
    #
    return NG message, -2, 200, req, res
  else
    raw-size = raw.length
    x = "#{raw-size}"
    INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => decompress to #{x.magenta} bytes"
    text = "#{raw}"
    try
      data = JSON.parse text
    catch error
      message = "#{profile}/#{id}/#{originalname} is invalid JSON data"
      ERR error, message
      # # Although the uploaded archive is not a valid json file,
      # we still need to accept it. Otherwise, the client will keep uploading
      # the corrupted archive file onto sensor-hub.
      #
      return NG message, -3, 200, req, res
    res.status 200 .json { code: 0, message: null, result: result }
    buffer = null
    raw = null
    {items} = data
    # console.log JSON.stringify items
    xs = [ (new DataItem profile, id, i) for i in items ]
    ys = [ (x.to-array!) for x in xs when x.is-broadcastable! ]
    zs = JSON.stringify ys
    size = "#{zs.length}"
    ratio = (zs.length * 100 / raw-size).toFixed 2
    INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => transform to #{size.magenta} bytes (#{ratio.red}%)"
    DUMP_ITEMS ys


PROCESS_JSON_GZ = (req, res) ->
  {file, params} = req
  {id, profile} = params
  return NG "invalid file upload form", -1, 400, req, res unless file?
  {fieldname, originalname, size} = file
  return NG "missing sensor_data_gz field", -1, 400, req, res unless fieldname == \sensor_data_gz
  if size is 0
    # Sometimes, the client might upload a sensor archive file with
    # zero bytes, because of possible disk failure. We need to accept
    # such improper request, and return HTTP/200 OKAY to client.
    #
    # If we responses HTTP/400 error to client, client will retry to
    # upload same corrupted sensor archive file (zero bytes) again and
    # again. That's why we responses HTTP/200 OKAY for such case.
    #
    message = "#{id}/#{profile}/#{originalname} but unexpected zero bytes"
    WARN message
    return NG message, -4, 200, req, res
  else
    {buffer} = file
    file.buffer = null
    return PROCESS_COMPRESSED_DATA originalname, buffer, id, profile, req, res


argv = global.argv = yargs
  .alias \p, \port
  .describe \p, 'port number to listen'
  .default \p, 7000
  .alias \v, \verbose
  .describe \v, 'enable verbose outputs'
  .default \v, no
  .alias \d, \dump
  .describe \d, 'enable data dump on console before forwarding'
  .default \d, no
  .demandOption <[port verbose dump]>
  .strict!
  .help!
  .argv

upload = multer {storage: multer.memoryStorage!}

web = express!
web.set 'trust proxy', true
web.post '/api/v1/hub/:id/:profile', (upload.single \sensor_data_gz), PROCESS_JSON_GZ

HOST = \0.0.0.0
PORT = argv.port
server = http.createServer web
server.on \listening -> INFO "listening port #{HOST}:#{PORT} ..."
server.listen PORT, HOST
