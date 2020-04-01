local Calls=require("TarantoolApi_Calls")
local bit=require('bit')
local MsgPack=require("cmsgpack")
local TarantoolApi=require("TarantoolApi")
local cjson=require("cjson")

return function(ngx)
  -- hide `{"params": [...]}` from a user
  ngx.req.read_body()
  local body = ngx.req.get_body_data()

  local Call=string.sub(ngx.var.uri,6)
  local CallData=Calls[Call]
  if not CallData then
    Call=string.sub(ngx.var.uri,6):sub(1,-2)
    CallData=Calls[Call]
  end
  local ParsedBody
  ngx.log(ngx.INFO,CallData)
  if CallData then
    if body and not pcall(function()
          ParsedBody="PUT"==ngx.req.get_method()and MsgPack.unpack(body)or cjson.decode(body)
        end)then
      ngx.status=400
      ngx.print('{"Error":{"Name":"No json"}}')
    else
      local Params={}
      local PreParams=ngx.req.get_uri_args(0)
      for i,k in pairs(CallData) do
        Params[i]=PreParams and PreParams[k]or ParsedBody and ParsedBody[k] or nil
      end

      local result1,result2=TarantoolApi[Call](Params)

      if result1~=500 then
        ngx.status=result1
        if not result2 then
          ngx.print("{}")
        elseif type(result2) == "string" then
          ngx.header["content_type"] = "text/plain"
          ngx.print(result2)
        elseif type(result2) == "table" then
          ngx.say("PUT"==ngx.req.get_method()and MsgPack.pack(result2)or cjson.encode(result2))
        elseif type(result2) == "number" then
          ngx.say("PUT"==ngx.req.get_method()and MsgPack.pack(result2)or tostring(result2))
        else
          ngx.status = 500
          ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Unexpected response from Tarantool",Data=cjson.encode(result2)}})or '{"Error":{"Name":"Unexpected response from Tarantool","Data":'..cjson.encode(result2).."}}")
        end
      else
        ngx.status=500
        ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Unexpected response from Tarantool",Data=cjson.encode(result2)}})or'{"Error":{"Name":"Tarantool does not work","Data":'..cjson.encode(result2).."}}")
      end
    end
  else
    ngx.status=405
    ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Method Not Allowed"}})or '{"Error":{"Name":"Method Not Allowed"}}')
  end
end
