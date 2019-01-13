#!/usr/bin/env lsc
#
require! <[fs path http zlib]>
require! <[colors yargs express multer prettyjson body-parser request]>
moment = require \moment-timezone

const ONE_MONTH = 30 * 24 * 60 * 60 * 1000
const ONE_HOUR = 60 * 60 * 1000
const NS2_FIELD_NAME = \sensor_csv_gz

NG = (message, code, status, req, res) ->
  {configs} = module
  url = req.originalUrl
  result = {url, code, message, configs}
  return res.status status .json result


INFO = (message) ->
  now = moment!
  console.log "#{now.format 'MMM/DD HH:mm:ss.SSS'} [INFO] #{message}"


ERR = (message) ->
  now = moment!
  console.log "#{now.format 'MMM/DD HH:mm:ss.SSS'} [ERR ] #{message}"



class DataNode
  (@profile, @id, @p) ->
    @kvs = {}
    @updated_at = null
    @time_shifts = null
    return

  show-message: (message) ->
    {profile, id, p} = self = @
    {verbose} = global.argv
    return unless verbose
    INFO "[#{id.yellow}] #{p.green} => #{message.gray}"

  update: (updated_at, data_type, value, time_shifts) ->
    {kvs} = self = @
    d1 = {updated_at, data_type, value, time_shifts}
    if not self.updated_at?
      self.updated_at = updated_at
      self.time_shifts = time_shifts
    d0 = kvs[data_type]
    self.show-message "#{data_type}: ignore old value `#{d0.value}` at #{d0.updated_at}ms" if d0?
    kvs[data_type] = d1

  serialize: ->
    {updated_at, p, kvs, time_shifts} = self = @
    # ys = [ "#{ts}" for ts in time_shifts ]
    # ys = ys.join ','
    ks = { [k, v.value] for k, v of kvs }
    # pairs = JSON.stringify ks
    pairs = [ "#{k}=#{v}" for k, v of ks ]
    xs = ["#{updated_at}", "#{time_shifts[0]}", p] ++ pairs
    return xs.join '\t'


class DataAggregator
  (@profile, @id) ->
    @pathes = {}
    return

  show-message: (message, ret=no, p="") ->
    {profile, id} = self = @
    {verbose} = global.argv
    return unless verbose
    INFO "[#{id.yellow}.#{updated_at}] #{p.green} => #{message.gray}"
    return ret

  update: (@items) ->
    {profile, id, pathes} = self = @
    for i in items
      [updated_at, board_type, board_id, sensor, data_type, value, time_shifts] = i
      p = "#{board_type}/#{board_id}/#{sensor}"
      node = pathes[p]
      node = new DataNode profile, id, p unless node?
      node.update updated_at, data_type, value, time_shifts
      pathes[p] = node

  serialize: ->
    {pathes} = self = @
    xs = [ (p.serialize!) for k, p of pathes ]
    return xs


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
    # [TODO]
    # 1. ignore when the value isn't changed, for past 60 seconds
    # 2. aggregate the last one value
    # 3. transform to new schema
    # 4. chain to next server

  to-array: ->
    {board_type, board_id, sensor, data_type, updated_at, value, time_shifts} = self = @
    return [updated_at, board_type, board_id, sensor, data_type, value, time_shifts]


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


SEND_TO_NEXT0 = (profile, id, file) ->
  {NS0} = process.env
  return unless NS0?
  url = "#{NS0}/api/v1/hub/#{id}/#{profile}"
  {fieldname, originalname, size, buffer, mimetype} = file
  filename = originalname
  data = sensor_data_gz: {value: buffer, options: {filename: filename, content-type: mimetype}}
  opts = {url: url, form-data: data}
  (err, rsp, body) <- request.post opts
  return ERR "failed to send to #{url}, #{err}" if err?
  return ERR "unexpected return code: #{rsp.statusCode}, for #{url}" unless rsp.statusCode is 200


SEND_TO_NEXT1 = (profile, id, items) ->
  {NS1} = process.env
  return unless NS1?
  url = "#{NS1}/next1/#{profile}/#{id}"
  req =
    url: url
    method: \POST
    json: yes
    body: items
  (err, rsp, body) <- request.post req
  return ERR "failed to send to #{url}, #{err}" if err?
  return ERR "unexpected return code: #{rsp.statusCode}, for #{url}" unless rsp.statusCode is 200
  return


SEND_TO_NEXT2 = (profile, id, bytes) ->
  {NS2} = process.env
  return unless NS2?
  formData =
    sensor_csv_gz:
      value: bytes
      options: {filename: "/tmp/#{profile}-#{id}.csv.gz", contentType: 'application/gzip'}
  url = "#{NS2}/next2/#{profile}/#{id}"
  opts = {url, formData}
  INFO "delivering #{bytes.length} bytes to #{url} ..."
  (err, rsp, body) <- request.post opts
  return ERR "failed to send to #{url}, #{err}" if err?
  return ERR "unexpected return code: #{rsp.statusCode}, for #{url}, body => #{body}" unless rsp.statusCode is 200
  return


PROCESS_COMPRESSED_DATA = (originalname, buffer, id, profile, req, res) ->
  received = (new Date!) - 0
  buffer-size = buffer.length
  x = "#{buffer-size}"
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
    return unless items? and items.length > 0
    xs = [ (new DataItem profile, id, i) for i in items ]
    ys = [ (x.to-array!) for x in xs when x.is-broadcastable! ]
    zs = JSON.stringify ys
    size = "#{zs.length}"
    ratio = (zs.length * 100 / raw-size).toFixed 2
    INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => transform to #{size.magenta} bytes (#{ratio.red}%)"
    DUMP_ITEMS ys
    SEND_TO_NEXT1 profile, id, ys
    transformed = (new Date!) - 0
    duration = transformed - received
    metadata = {profile, id, received, transformed, duration}
    da = new DataAggregator profile, id
    da.update ys
    ds = da.serialize!
    ds = ["#\t#{(JSON.stringify metadata)}"] ++ ds
    if global.argv.dump
      [ console.log "\t#{d}" for d in ds ]
    data = ds.join '\n'
    size = "#{data.length}"
    ratio = (data.length * 100 / raw-size).toFixed 2
    INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => compact to #{size.magenta} bytes (#{ratio.red}%)"
    zs = new Buffer data
    (err, bytes) <- zlib.gzip zs
    return ERR "unexpected error when compressing #{profile}/#{id}/#{originalname}, err: #{err}" if err?
    size = "#{bytes.length}"
    ratio = (bytes.length * 100 / buffer-size).toFixed 2
    INFO "#{profile.cyan}/#{id.yellow}/#{originalname.green} => compress to #{size.magenta} bytes (#{ratio.red}%)"
    SEND_TO_NEXT2 profile, id, bytes


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
	# file.buffer = null
    PROCESS_COMPRESSED_DATA originalname, buffer, id, profile, req, res
    SEND_TO_NEXT0 profile, id, file
    file.buffer = null



PROCESS_CSV_GZ = (req, res) ->
  {file, params} = req
  {id, profile} = params
  return NG "invalid file upload form", -1, 400, req, res unless file?
  {fieldname, originalname, size} = file
  return NG "missing #{NS2_FIELD_NAME} field", -1, 400, req, res unless fieldname is NS2_FIELD_NAME
  return if size is 0
  {buffer} = file
  file.buffer = null
  return PROCESS_NEXT2 profile, id, buffer, size, req, res


PROCESS_NEXT1 = (req, res) ->
  {body, params} = req
  {profile, id} = params
  INFO "next1-server => #{profile.cyan}/#{id.yellow}"
  res.end!
  return DUMP_ITEMS body


PROCESS_NEXT2 = (profile, id, buffer, size, req, res) ->
  now = new Date!
  INFO "next2-server => #{profile.cyan}/#{id.yellow}: receive #{size} bytes"
  (zerr, raw) <- zlib.gunzip buffer
  if zerr?
    message = "#{profile}/#{id}/#{originalname} decompression failure."
    ERR zerr, message
    return NG message, -2, 400, req, res
  else
    res.status 200 .end!
    text = "#{raw}"
    xs = text.split '\n'
    [ console.log "\t#{x}" for x in xs ]
    console.log "\t--------------------"
    metadata = xs.shift!
    for x in xs
      [ms, shift1, p, ...kvs] = tokens = x.split '\t'
      timestamp = parse-int ms
      shift2 = now - timestamp
      timestamp = new Date timestamp
      timestamp = "#{moment timestamp .format 'MMM/DD HH:mm:ss.SSS'} (#{ms.gray})"
      [board_type, board_id, sensor] = pathes = p.split '/'
      ys = [''] ++ [timestamp, "#{board_type.green}/#{board_id.green}/#{sensor.green}"]
      prefix = ys.join '\t'
      ys = ["#{shift1}ms".yellow, "#{shift2}ms".magenta]
      postfix = ys.join '\t'
      ks = [ (k.split '=') for k in kvs ]
      [ console.log "#{prefix}\t#{k[0].green}\t#{k[1].cyan}\t\t\t#{postfix}" for k in ks ]



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
j = body-parser.json!

web = express!
web.set 'trust proxy', true
web.post '/api/v1/hub/:id/:profile', (upload.single \sensor_data_gz), PROCESS_JSON_GZ
web.post '/next1/:profile/:id', j, PROCESS_NEXT1
web.post '/next2/:profile/:id', (upload.single NS2_FIELD_NAME), PROCESS_CSV_GZ

HOST = \0.0.0.0
PORT = argv.port
server = http.createServer web
server.on \listening -> INFO "listening port #{HOST}:#{PORT} ..."
server.listen PORT, HOST
