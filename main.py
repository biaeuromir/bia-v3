#!/usr/bin/env python3
# BIA v3 Sprint 1 — Production Foundation
import os, re, json, time, uuid, logging, random
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass, field
from fastapi import FastAPI, Request
import httpx

SUPA=os.getenv("SUPABASE_URL",""); SK=os.getenv("SUPABASE_KEY","")
EVO=os.getenv("EVOLUTION_URL",""); EK=os.getenv("EVOLUTION_KEY","")
INSTANCE=os.getenv("EVOLUTION_INSTANCE","EuromirBia")
PYTHON_URL=os.getenv("PYTHON_FICHAJES_URL",""); N8N=os.getenv("N8N_URL","")
OPENAI_KEY=os.getenv("OPENAI_API_KEY",""); PORT=int(os.getenv("PORT","8001"))

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log=logging.getLogger("bia-v3")
app=FastAPI(title="BIA v3",version="3.1")

@dataclass
class BiaState:
    trace_id:str=""; telefono:str=""; mensaje_original:str=""; mensaje_normalizado:str=""
    tipo_mensaje:str="texto"; empleado:dict=field(default_factory=dict); historial:str=""
    dominio:str=""; dominio_fuente:str=""; accion:str=""; respuesta:str=""
    confianza:float=1.0; necesita_humano:bool=False; errores:list=field(default_factory=list)
    metadata:dict=field(default_factory=dict); timestamps:dict=field(default_factory=dict); duracion_ms:int=0
    def timer_start(self,s): self.timestamps[f"{s}_start"]=time.time()
    def timer_end(self,s): self.timestamps[f"{s}_ms"]=int((time.time()-self.timestamps.get(f"{s}_start",time.time()))*1000)
    def add_error(self,e,rec=True): self.errores.append({"error":e,"recoverable":rec}); log.error(f"[{self.trace_id}] {e}")

async def db_get(t,q=""):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.get(f"{SUPA}/rest/v1/{t}?{q}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"}); return r.json() if r.status_code==200 else []
    except: return []

async def db_post(t,d):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.post(f"{SUPA}/rest/v1/{t}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json=d)
            return r.json() if r.status_code in(200,201) else {"error":r.text}
    except Exception as e: return {"error":str(e)}

async def guardar_ejecucion(s):
    await db_post("bia_ejecuciones",{"trace_id":s.trace_id,"telefono":s.telefono,"empleado_id":s.empleado.get("id"),
        "empleado_nombre":s.empleado.get("nombre",""),"input_original":(s.mensaje_original or "")[:2000],
        "input_normalizado":(s.mensaje_normalizado or "")[:2000],"tipo_mensaje":s.tipo_mensaje,"dominio":s.dominio,
        "dominio_fuente":s.dominio_fuente,"accion":s.accion,"confianza":s.confianza,"agente":s.dominio,
        "estado_final":"error" if s.errores else "ok","respuesta":(s.respuesta or "")[:2000],
        "necesita_humano":s.necesita_humano,"error":json.dumps(s.errores) if s.errores else None,
        "duracion_ms":s.duracion_ms,"metadata":json.dumps({"timestamps":s.timestamps})})

async def wa(num,txt):
    try:
        async with httpx.AsyncClient(timeout=15) as c: await c.post(f"{EVO}/message/sendText/{INSTANCE}",headers={"apikey":EK,"Content-Type":"application/json"},json={"number":num,"text":txt})
    except Exception as e: log.error(f"WA error: {e}")

async def gpt(prompt,system="",model="gpt-4o-mini",max_t=500):
    try:
        msgs=[];
        if system: msgs.append({"role":"system","content":system})
        msgs.append({"role":"user","content":prompt})
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post("https://api.openai.com/v1/chat/completions",headers={"Authorization":f"Bearer {OPENAI_KEY}","Content-Type":"application/json"},
                json={"model":model,"messages":msgs,"max_tokens":max_t,"temperature":0.3})
            return r.json()["choices"][0]["message"]["content"]
    except Exception as e: log.error(f"GPT error: {e}"); return ""

# DETECTOR DETERMINISTA
PF=[re.compile(r'\d{1,2}[:.h]\d{0,2}\s*(a|hasta|-)\s*\d{1,2}',re.I),re.compile(r'de\s+\d{1,2}\s+(a|hasta)\s+\d{1,2}',re.I),
    re.compile(r'\d{1,2}\s*-\s*\d{1,2}'),re.compile(r'empiezo|empezado|llego|ya estoy|comienzo',re.I),
    re.compile(r'salgo|termino|terminado|acabado|me voy',re.I),re.compile(r'ficho|fichar|fichaje',re.I),re.compile(r'hoy\s+\d{1,2}',re.I)]
PS=re.compile(r'^(hola|buenos d[ií]as|buenas( tardes| noches)?|qu[eé] tal|hey)[\s!.?]*$',re.I)
PC=re.compile(r'\b(confirmo|todos ok|confirmado)\b',re.I)

def detectar(txt):
    t=txt.lower().strip()
    if any(p.search(t) for p in PF): return "FICHAJE","procesar",1.0
    if PC.search(t): return "FICHAJE","confirmar",0.9
    if PS.match(t): return "SALUDO","responder",1.0
    return "AMBIGUO","clasificar",0.0

# CLASIFICADOR GPT
async def clasificar(txt):
    p=f'Clasifica en UN dominio y da confianza. Dominios: FICHAJE,OBRAS,FINANZAS,EMPLEADOS,DOCUMENTOS,INVENTARIO,GENERAL. JSON: {{"dominio":"OBRAS","confianza":0.85}}. Mensaje: "{txt[:500]}"'
    raw=await gpt(p)
    try:
        if "```" in raw: raw=raw.split("```")[1].replace("json","").strip()
        d=json.loads(raw.strip()); dom=d.get("dominio","GENERAL").upper(); conf=float(d.get("confianza",0.5))
        return (dom if dom in ["FICHAJE","OBRAS","FINANZAS","EMPLEADOS","DOCUMENTOS","INVENTARIO","GENERAL"] else "GENERAL"),conf
    except: return "GENERAL",0.3

# AGENTES
async def ag_fichaje(s):
    s.timer_start("fichaje"); ep="/confirmar-fichajes" if s.accion=="confirmar" else "/procesar-fichaje"
    body={"mensaje":s.mensaje_normalizado,"empleado_id":s.empleado["id"],"empleado_nombre":s.empleado["nombre"],"empleado_telefono":s.empleado.get("telefono",""),"coste_hora":s.empleado.get("coste_hora",0),"fuera_madrid_hora":15}
    if s.accion=="confirmar": body={"respuesta":s.mensaje_normalizado,"fecha":""}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(f"{PYTHON_URL}{ep}",json=body)
            d=r.json()
        msg=d.get("mensaje",d.get("message",str(d)))
        if "obra" in msg.lower() and "1." in msg:
            log.info(f"[{s.trace_id}] Saving espera")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"ok":True}})
        s.timer_end("fichaje")
        return msg
    except Exception as e: s.add_error(f"Fichaje: {e}"); s.timer_end("fichaje"); return "Problema con el fichaje. Repite por favor 🔧"

async def ag_saludo(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre","compañero")
    return random.choice([f"¡Buenas, {n}! ¿En qué te ayudo? 😊",f"¡Ey, {n}! Dime 💪",f"¡Hola {n}! Aquí estoy 🏗️"])

async def ag_obras(s):
    s.timer_start("obras"); obras=await db_get("obras","select=id,nombre,estado,direccion,presupuesto_total&estado=eq.En curso&order=nombre")
    n=s.empleado.get("apodo") or s.empleado.get("nombre",""); r=await gpt(f"Obras en curso:\n{json.dumps(obras,ensure_ascii=False)[:2000]}\n\nPregunta: \"{s.mensaje_normalizado}\"\nCorto para WhatsApp.",f"Eres Bia de Euromir. Hablas con {n}.","gpt-4o")
    s.timer_end("obras"); return r or "No pude consultar obras. 🔧"

async def ag_finanzas(s):
    s.timer_start("finanzas"); g=await db_get("gastos","select=id,concepto,total,obra,proveedor&order=created_at.desc&limit=10")
    r=await gpt(f"Gastos recientes:\n{json.dumps(g,ensure_ascii=False)[:1500]}\n\nPregunta: \"{s.mensaje_normalizado}\"","Eres Bia. Corto.","gpt-4o")
    s.timer_end("finanzas"); return r or "No pude consultar finanzas. 🔧"

async def ag_empleados(s):
    s.timer_start("emps"); e=await db_get("empleados","select=id,nombre,cargo,estado&estado=eq.activo&order=nombre")
    r=await gpt(f"Empleados:\n{json.dumps(e,ensure_ascii=False)[:1500]}\n\nPregunta: \"{s.mensaje_normalizado}\"","Eres Bia. Corto.","gpt-4o")
    s.timer_end("emps"); return r or "No pude consultar empleados. 🔧"

async def ag_general(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre",""); nt=s.empleado.get("notas_bia","")[:300]
    return await gpt(s.mensaje_normalizado,f"Eres Bia, secretaria de Euromir. Cercana, directa. Hablas con {n}. Notas: {nt}. CORTO para WhatsApp.","gpt-4o") or f"Perdona {n}, no te entendí 🤔"

AG={"FICHAJE":ag_fichaje,"SALUDO":ag_saludo,"OBRAS":ag_obras,"FINANZAS":ag_finanzas,"EMPLEADOS":ag_empleados,"DOCUMENTOS":ag_general,"INVENTARIO":ag_general,"GENERAL":ag_general}

# ROUTER
async def procesar(s):
    t0=time.time(); s.trace_id=str(uuid.uuid4())[:8]; s.mensaje_normalizado=s.mensaje_original.strip()
    log.info(f"[{s.trace_id}] 📩 {s.empleado.get('nombre','?')}: {s.mensaje_original[:80]}")
    # Comprobar espera activa
    esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&order=created_at.desc&limit=1")
    if esperas:
        esp=esperas[0]; log.info(f"[{s.trace_id}] \u23f3 Espera activa: {esp['tipo']}")
        try:
            async with httpx.AsyncClient(timeout=10) as c: await c.delete(f"{SUPA}/rest/v1/bia_esperas?id=eq.{esp['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"})
        except: pass
        s.dominio=esp["dominio"]; s.dominio_fuente="espera"; s.confianza=1.0; s.accion="continuar"
        s.timer_start("agente")
        try: s.respuesta=await AG.get(s.dominio,ag_general)(s)
        except Exception as e: s.add_error(f"Espera: {e}"); s.respuesta="Problema t\u00e9cnico \U0001f527"
        s.timer_end("agente"); s.duracion_ms=int((time.time()-t0)*1000)
        await guardar_ejecucion(s); return s
    s.timer_start("detector"); dom,acc,conf=detectar(s.mensaje_normalizado); s.timer_end("detector")
    if dom!="AMBIGUO":
        s.dominio,s.accion,s.confianza,s.dominio_fuente=dom,acc,conf,"regex"
        log.info(f"[{s.trace_id}] 🎯 Regex: {dom}")
    else:
        s.timer_start("clasificador"); dom,conf=await clasificar(s.mensaje_normalizado); s.timer_end("clasificador")
        s.dominio,s.confianza,s.dominio_fuente=dom,conf,"gpt"
        log.info(f"[{s.trace_id}] 🤖 GPT: {dom} ({conf})")
    if not s.empleado.get("id"): s.add_error("Sin empleado",False); s.respuesta="No te identifiqué."; s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
    if s.confianza<0.3 and s.dominio not in("GENERAL","SALUDO"): s.necesita_humano=True
    s.timer_start("agente")
    try: s.respuesta=await AG.get(s.dominio,ag_general)(s)
    except Exception as e: s.add_error(f"Agente: {e}"); s.respuesta="Problema técnico. Repite por favor 🔧"
    s.timer_end("agente"); s.duracion_ms=int((time.time()-t0)*1000)
    log.info(f"[{s.trace_id}] ✅ {s.dominio} {s.duracion_ms}ms")
    await guardar_ejecucion(s); return s

@app.post("/webhook")
async def webhook(req:Request):
    try:
        data=await req.json(); event=data.get("event","")
        if event not in("messages.upsert",""): return {"ok":True}
        msg=data.get("data",{}); remote=msg.get("key",{}).get("remoteJid",""); fm=msg.get("key",{}).get("fromMe",False)
        if fm: return {"ok":True}
        tel=remote.replace("@s.whatsapp.net","")
        cont=msg.get("message",{}).get("conversation","") or msg.get("message",{}).get("extendedTextMessage",{}).get("text","")
        if not cont or not tel: return {"ok":True}
        emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
        if not emps: await wa(f"{tel}@s.whatsapp.net","No estás registrado. Contacta con tu encargado."); return {"ok":True}
        s=BiaState(telefono=tel,mensaje_original=cont,empleado=emps[0]); s=await procesar(s)
        if s.respuesta: await wa(f"{tel}@s.whatsapp.net",s.respuesta)
        return {"ok":True,"trace_id":s.trace_id}
    except Exception as e: log.error(f"Webhook: {e}"); return {"ok":False,"error":str(e)}

@app.post("/test")
async def test(req:Request):
    d=await req.json(); emp=d.get("empleado",{"id":1,"nombre":"Test","telefono":"0","apodo":"Test","rol":1,"coste_hora":20})
    s=BiaState(telefono=emp.get("telefono","0"),mensaje_original=d.get("mensaje","hola"),empleado=emp); s=await procesar(s)
    return {"trace_id":s.trace_id,"dominio":s.dominio,"dominio_fuente":s.dominio_fuente,"confianza":s.confianza,
        "respuesta":s.respuesta,"necesita_humano":s.necesita_humano,"errores":s.errores,"duracion_ms":s.duracion_ms,"timestamps":s.timestamps}

@app.get("/health")
async def health(): return {"status":"ok","service":"bia-v3","version":"3.3-fix"}

if __name__=="__main__":
    import uvicorn; uvicorn.run(app,host="0.0.0.0",port=PORT)
