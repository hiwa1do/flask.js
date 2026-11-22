class Flask{constructor(){this.routes=[];this.websockets=[];this.errors=new Map()}compile(path){const keys=[],pattern=(path.startsWith("/")?path:"/"+path).split("/").map(s=>{const typed=s.match(/^<(int|string|float|path):([\w$]+)>$/),simple=s.match(/^<([\w$]+)>$/)||s.match(/^:([\w$]+)$/),p=typed||simple;if(p){const type=typed?p[1]:"string",name=typed?p[2]:p[1];keys.push({name,type});if(type==="int")return"(-?\\d+)";if(type==="float")return"(-?(?:\\d+\\.?\\d*|\\.\\d+))";if(type==="path")return"(.*)";return"([^/]+)"}if(s==="*"){keys.push({name:"wildcard",type:"path"});return"(.*)"}return s.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")}).join("/");return{keys,regex:new RegExp(`^${pattern}/?$`)}}getParams(match,keys){const params={};keys.forEach((key,i)=>{let value=match[i+1];try{value=decodeURIComponent(value)}catch{}if(key.type==="int"||key.type==="float")value=Number(value);params[key.name]=value});return params}route(path,options={}){return handler=>{const compiled=this.compile(path);this.routes.push({path,keys:compiled.keys,regex:compiled.regex,methods:(options.methods||["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"]).map(m=>m.toUpperCase()),handler});return this}}websocket(path){return handler=>{const compiled=this.compile(path);this.websockets.push({path,keys:compiled.keys,regex:compiled.regex,handler});return this}}errorhandler(code){return handler=>{this.errors.set(Number(code),handler);return this}}async parseBody(request){if(!request.body)return null;const type=request.headers.get("Content-Type")||"";if(type.includes("multipart/form-data"))return request.formData();if(type.includes("application/x-www-form-urlencoded"))return Object.fromEntries(new URLSearchParams(await request.text()));if(type.includes("json"))return request.json();if(type.startsWith("text/"))return request.text();return request.arrayBuffer()}normalize(result){if(result instanceof Response)return result;if(result===undefined||result===null)return new Response(null);if(typeof result==="string")return this.html(result);if(typeof result==="number"||typeof result==="boolean")return this.text(result);return this.json(result)}json(data,status=200,headers={}){const h=new Headers(headers);if(!h.has("Content-Type"))h.set("Content-Type","application/json; charset=utf-8");return new Response(JSON.stringify(data),{status,headers:h})}text(data,status=200,headers={}){const h=new Headers(headers);if(!h.has("Content-Type"))h.set("Content-Type","text/plain; charset=utf-8");return new Response(String(data),{status,headers:h})}html(data,status=200,headers={}){const h=new Headers(headers);if(!h.has("Content-Type"))h.set("Content-Type","text/html; charset=utf-8");return new Response(data,{status,headers:h})}redirect(location,status=302,base="https://localhost"){return new Response(null,{status,headers:{Location:new URL(location,base).toString()}})}response(body,status=200,headers={}){return new Response(body,{status,headers})}async handleError(code,error,data){const handler=this.errors.get(Number(code));if(handler){try{return this.normalize(await handler({...data,error,status:code,code}))}catch(e){console.error("Error handler failed:",e)}}const messages={400:"Bad Request",404:"Not Found",405:"Method Not Allowed",500:"Internal Server Error"};return this.text(messages[code]||"Error",code,code===405?{Allow:(data.allow||[]).join(", ")}:{})}async handleWebSocket(request,env,ctx,route,match){if(request.headers.get("Upgrade")?.toLowerCase()!=="websocket")return new Response("Expected WebSocket connection",{status:426});const pair=new WebSocketPair(),[clientSocket,serverSocket]=Object.values(pair);serverSocket.accept();const url=new URL(request.url),id=crypto.randomUUID(),listeners=new Map(),params=this.getParams(match,route.keys),dispatch=async(event,...args)=>{for(const callback of listeners.get(event)||[])try{await callback(...args)}catch(error){console.error(`WebSocket ${event} handler failed:`,error)}};const ws={id,request,url,path:url.pathname,params,env,ctx,socket:serverSocket,get readyState(){return serverSocket.readyState},get connected(){return serverSocket.readyState===WebSocket.OPEN},on(event,callback){if(!listeners.has(event))listeners.set(event,[]);listeners.get(event).push(callback);return ws},onOpen(callback){return ws.on("open",callback)},onMessage(callback){return ws.on("message",callback)},onJSON(callback,onError){return ws.onMessage((message,event)=>{try{if(typeof message!=="string")throw new TypeError("Expected a JSON message");callback(JSON.parse(message),event)}catch(error){if(onError)onError(error,event);else dispatch("error",error)}})},onClose(callback){return ws.on("close",callback)},onError(callback){return ws.on("error",callback)},send(data){if(serverSocket.readyState!==WebSocket.OPEN)return false;const payload=typeof data==="string"||data instanceof ArrayBuffer||ArrayBuffer.isView(data)?data:JSON.stringify(data);if(payload===undefined)return false;try{serverSocket.send(payload);return true}catch(error){console.error("WebSocket send failed:",error);return false}},sendJSON(data){return ws.send(JSON.stringify(data))},close(code=1000,reason=""){serverSocket.close(code,reason)}};serverSocket.addEventListener("message",event=>dispatch("message",event.data,event));serverSocket.addEventListener("close",event=>dispatch("close",event));serverSocket.addEventListener("error",event=>dispatch("error",event));try{await route.handler(ws);await dispatch("open",ws)}catch(error){console.error("WebSocket route failed:",error);try{serverSocket.close(1011,"WebSocket handler failed")}catch{}}return new Response(null,{status:101,webSocket:clientSocket})}async fetch(request,env,ctx){const url=new URL(request.url);for(const route of this.websockets){const match=url.pathname.match(route.regex);if(match)return this.handleWebSocket(request,env,ctx,route,match)}const method=request.method.toUpperCase(),base={request,method,url,path:url.pathname,args:url.searchParams,query:url.searchParams,headers:request.headers,env,ctx};let matched=false;const allowed=new Set();for(const route of this.routes){const match=url.pathname.match(route.regex);if(!match)continue;matched=true;route.methods.forEach(m=>allowed.add(m));if(!route.methods.includes(method)&&!(method==="HEAD"&&route.methods.includes("GET")))continue;const data={...base,params:this.getParams(match,route.keys),body:null,json:(d,s,h)=>this.json(d,s,h),text:(d,s,h)=>this.text(d,s,h),html:(d,s,h)=>this.html(d,s,h),redirect:(l,s)=>this.redirect(l,s,url),response:(b,s,h)=>this.response(b,s,h)};try{data.body=await this.parseBody(request);const response=this.normalize(await route.handler(data));return method==="HEAD"?new Response(null,{status:response.status,headers:response.headers}):response}catch(error){console.error("Route error:",error);return this.handleError(error instanceof SyntaxError?400:500,error,data)}}if(matched){if(allowed.has("GET"))allowed.add("HEAD");allowed.add("OPTIONS");return this.handleError(405,new Error("Method Not Allowed"),{...base,allow:[...allowed]})}return this.handleError(404,new Error("Not Found"),base)}}

const app = new Flask();

app.route("/")((data) => {
    return "Hello World!";
});

app.route("/post")((data) => {
    if (data.method === "POST") {
        return data.json({
            received: data.body
        }, 201);
    }
    return data.text("Method not supported", 405);
});

app.route("/<name>")((data) => {
    return `${data.params.name}`;
});

app.websocket("/ws")((ws) => {
    console.log("WebSocket connected:", ws.id);

    ws.onOpen(() => {
        ws.send("Connected to WebSocket");
    });

    ws.onMessage((message) => {
        ws.send(`Echo: ${message}`);
    });

    ws.onClose((event) => {
        return;
    });

    ws.onError((error) => {
        return;
    });
});

app.errorhandler(400)((data) => {
    return data.json({
        error: "Bad Request",
        message: data.error.message
    }, 400);
});

app.errorhandler(404)((data) => {
    return data.html(`
        <!doctype html>
        <html lang="en">
            <title>404 Not Found</title>
            <h1>Not Found</h1>
            <p>The requested URL was not found on the server.</p>
            <p>${data.path}</p>
        </html>
    `, 404);
});

app.errorhandler(405)((data) => {
    return data.json({
        error: "Method Not Allowed",
        method: data.method,
        allowed: data.allow
    }, 405);
});

app.errorhandler(500)((data) => {
    return data.html(`
        <!doctype html>
        <html lang="en">
            <title>500 Internal Server Error</title>
            <h1>Internal Server Error</h1>
        </html>
    `, 500);
});

export default {
    fetch(request, env, ctx) {
        return app.fetch(request, env, ctx);
    }
};
