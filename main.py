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
N8N_WEBHOOK=os.getenv("N8N_WEBHOOK","https://euromir-n8n.wp2z39.easypanel.host/webhook/whatsapp-euromir")
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
PF=[re.compile(r'\d{1,2}[:.h]\d{0,2}\s*(a|h?asta|-)\s*\d{1,2}',re.I),re.compile(r'de\s+\d{1,2}\s+(a|h?asta)\s+\d{1,2}',re.I),
    re.compile(r'\d{1,2}\s*-\s*\d{1,2}'),re.compile(r'empiezo|empezado|llego|ya estoy|comienzo',re.I),
    re.compile(r'salgo|termino|terminado|acabado|me voy',re.I),re.compile(r'ficho|fichar|fichaje',re.I),re.compile(r'hoy\s+\d{1,2}',re.I)]
PS=re.compile(r'^(hola|buenos d[ií]as|buenas( tardes| noches)?|qu[eé] tal|hey)[\s!.?]*$',re.I)
PC=re.compile(r'\b(confirmo|todos ok|confirmado)\b',re.I)

def detectar(txt):
    t=txt.lower().strip()
    if any(p.search(t) for p in PF): return "FICHAJE","procesar",1.0
    if PC.search(t): return "FICHAJE","confirmar",0.9
    if PS.match(t): return "SALUDO","responder",1.0
    if re.search(r'nueva obra|registra(r|me)?\s*obra|abrir obra|alta obra|dar de alta obra', t): return "OBRA_ALTA","crear",0.95
    if re.search(r'cerrar obra|baja obra|dar de baja', t): return "OBRA_BAJA","cerrar",0.9
    return "AMBIGUO","clasificar",0.0


def normalizar_horas(texto):
    """Normaliza horas: '6 de la tarde'→'18', 'de 8 a 5'→'de 8 a 17'"""
    import re
    t = texto
    # "X de la tarde/noche" → X+12
    def tarde_fix(m):
        h = int(m.group(1))
        return str(h + 12 if h < 12 else h)
    t = re.sub(r'(\d{1,2})\s*(?:de la tarde|de la noche|pm|p\.m\.)', tarde_fix, t, flags=re.I)
    t = re.sub(r'(\d{1,2})\s*(?:de la mañana|am|a\.m\.)', r'\1', t, flags=re.I)
    # Smart PM: "de X a Y" where Y < X and Y < 13 → Y is PM
    def smart_pm(m):
        pre = m.group(1)
        h1 = int(m.group(2))
        mid = m.group(3)
        h2 = int(m.group(4))
        rest = m.group(5) or ""
        if h2 < h1 and h2 < 13:
            h2 += 12
        return f"{pre}{h1}{mid}{h2}{rest}"
    t = re.sub(r'(de\s+)(\d{1,2})(\s*(?:a|h?asta|-)\s*)(\d{1,2})(\b)', smart_pm, t, flags=re.I)
    return t

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
    notas=s.empleado.get("notas_bia","")[:200]
    return await gpt(f"El empleado {n} te saluda: \"{s.mensaje_normalizado}\". Responde un saludo corto y cercano.",f"Eres Bia, secretaria de Euromir. Cercana, directa, con chispa. Hablas con {n}. Notas: {notas}. Max 2 líneas para WhatsApp.","gpt-4o-mini",100) or f"¡Buenas, {n}! Dime 💪"

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


async def ag_obra_alta(s):
    """Multi-step obra creation flow"""
    # Check which step we're on based on espera context
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_nombre","dominio":"OBRA_ALTA","contexto":{}})
    return "¡Vamos a abrir una obra! \U0001f3d7\ufe0f ¿Cómo se llama?"

async def ag_obra_baja(s):
    """Close an obra"""
    obras=await db_get("obras","select=id,nombre&estado=eq.En curso&order=nombre")
    lista="\n".join([f"{i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_baja","dominio":"OBRA_BAJA","contexto":{"obras_ids":[o["id"] for o in obras]}})
    return f"¿Qué obra quieres cerrar?\n\n{lista}\n\nDime número o nombre"

AG={"FICHAJE":ag_fichaje,"OBRA_ALTA":ag_obra_alta,"OBRA_BAJA":ag_obra_baja,"SALUDO":ag_saludo,"OBRAS":ag_obras,"FINANZAS":ag_finanzas,"EMPLEADOS":ag_empleados,"DOCUMENTOS":ag_general,"INVENTARIO":ag_general,"GENERAL":ag_general}

# ROUTER
async def procesar(s):
    t0=time.time(); s.trace_id=str(uuid.uuid4())[:8]; s.mensaje_normalizado=normalizar_horas(s.mensaje_original.strip())
    log.info(f"[{s.trace_id}] 📩 {s.empleado.get('nombre','?')}: {s.mensaje_original[:80]}")
    # Comprobar espera activa
    esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&order=created_at.desc&limit=1")
    if esperas:
        esp=esperas[0]; log.info(f"[{s.trace_id}] \u23f3 Espera activa: {esp['tipo']}")
        try:
            async with httpx.AsyncClient(timeout=10) as c: await c.delete(f"{SUPA}/rest/v1/bia_esperas?id=eq.{esp['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"})
        except: pass
        # If n8n is waiting for a response, forward text to n8n
        if esp.get("tipo")=="factura_obra":
            log.info(f"[{s.trace_id}] Factura obra selection: {s.mensaje_normalizado}")
            ctx=esp.get("contexto",{}) or {}
            factura=ctx.get("factura",{})
            obras_ids=ctx.get("obras",[])
            # Parse selection
            try:
                sel=int(s.mensaje_normalizado.strip())-1
                obras_full=await db_get("obras","select=id,nombre&estado=eq.En curso&order=nombre")
                if 0<=sel<len(obras_full):
                    obra=obras_full[sel]
                    # Register gasto
                    # Calculate trimestre
                    from datetime import datetime as dt
                    try:
                        fdate=factura.get("fecha","2026-01-01")
                        if fdate and len(fdate)>=7:
                            m=int(fdate[5:7]); y=fdate[:4]
                            trim=f"T{(m-1)//3+1}-{y}"
                        else: trim="T1-2026"
                    except: trim="T1-2026"
                    gasto={"obra_id":obra["id"],"obra":obra["nombre"],"empleado_id":s.empleado.get("id",0),"empleado_nombre":s.empleado.get("nombre",""),"proveedor":factura.get("proveedor",""),"cif_proveedor":factura.get("CIF",factura.get("cif","")),"numero_factura":str(factura.get("numero_factura",factura.get("numero",""))),"fecha_factura":factura.get("fecha",None),"concepto":factura.get("concepto",""),"base_imponible":factura.get("base_imponible",0),"tipo_iva":factura.get("iva_porcentaje",21),"cuota_iva":factura.get("iva_importe",0),"irpf":0,"total":factura.get("total",0),"trimestre":trim}
                    result=await db_post("gastos",gasto)
                    if "error" in str(result): log.error(f"Gastos insert: {result}")
                    prov=factura.get("proveedor","?"); tot=factura.get("total",0)
                    # Write to n8n for Sheets + Drive + Email
                    try:
                        sheet_data={"spreadsheet_id":obra.get("spreadsheet_id",""),"obra_nombre":obra["nombre"],"obra_id":obra["id"],"proveedor":prov,"concepto":factura.get("concepto",""),"base":factura.get("base_imponible",0),"iva":factura.get("iva_importe",0),"total":tot,"fecha":factura.get("fecha",""),"trimestre":trim,"empleado":s.empleado.get("nombre","")}
                        async with httpx.AsyncClient(timeout=15) as sc:
                            await sc.post(f"{N8N}/webhook/escribir-gasto-sheet",json=sheet_data)
                            log.info("Sheet write sent to n8n")
                    except Exception as e: log.error(f"Sheet write: {e}")
                    s.respuesta=f"✅ Gasto registrado!\n\nProveedor: {prov}\nTotal: {tot}€\nObra: {obra['nombre']}\nTrimestre: {trim}\n\nGuardado en BD + Sheet ✅"
                else:
                    s.respuesta="Número no válido. Repite por favor."
            except:
                s.respuesta="No entendí. Dime el número de la obra."
            s.dominio="FACTURA"; s.dominio_fuente="espera"; s.duracion_ms=int((time.time()-t0)*1000)
            await guardar_ejecucion(s); return s
        # OBRA ALTA - multi-step flow
        if esp.get("tipo")=="obra_nombre":
            ctx=esp.get("contexto",{}) or {}
            ctx["nombre"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_direccion","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="¿Dirección de la obra?"; s.dominio="OBRA_ALTA"; s.dominio_fuente="espera"
            s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
        if esp.get("tipo")=="obra_direccion":
            ctx=esp.get("contexto",{}) or {}
            ctx["direccion"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_madrid","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="¿Dentro o fuera de Madrid?"; s.dominio="OBRA_ALTA"; s.dominio_fuente="espera"
            s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
        if esp.get("tipo")=="obra_madrid":
            ctx=esp.get("contexto",{}) or {}
            ctx["fuera_madrid"]="fuera" in s.mensaje_normalizado.lower()
            # Get encargados list
            encs=await db_get("empleados","select=id,nombre,rol&rol=in.(1,2)&estado=eq.activo&order=nombre")
            lista="\n".join([f"{i+1}. {e['nombre']}" + (" (Admin)" if e['rol']==1 else "") for i,e in enumerate(encs)])
            ctx["encargados"]=[e["id"] for e in encs]
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_encargado","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta=f"¿Quién es el encargado?\n\n{lista}\n\nDime número o nombre"; s.dominio="OBRA_ALTA"; s.dominio_fuente="espera"
            s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
        if esp.get("tipo")=="obra_encargado":
            ctx=esp.get("contexto",{}) or {}
            # Parse encargado selection
            encs=await db_get("empleados","select=id,nombre,rol&rol=in.(1,2)&estado=eq.activo&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1
                enc=encs[sel] if 0<=sel<len(encs) else encs[0]
            except:
                enc=next((e for e in encs if s.mensaje_normalizado.lower() in e["nombre"].lower()),encs[0])
            # Call WF-15 via webhook
            try:
                obra_data={"nombre":ctx.get("nombre",""),"direccion":ctx.get("direccion",""),"tipo":"reforma","presupuesto":0,"telefono":s.empleado.get("telefono",""),"encargado_id":enc["id"],"encargado_nombre":enc["nombre"],"fuera_madrid":ctx.get("fuera_madrid",False)}
                log.info(f"[{s.trace_id}] Creating obra: {obra_data}")
                async with httpx.AsyncClient(timeout=60) as oc:
                    wr=await oc.post(f"{N8N}/webhook/alta-obra",json=obra_data)
                    log.info(f"WF-15 response: {wr.status_code}")
                nombre_o=ctx.get("nombre",""); dir_o=ctx.get("direccion",""); fm="Fuera de Madrid" if ctx.get("fuera_madrid") else "Madrid"
                s.respuesta=f"✅ Obra creada!\n\n🏗 {nombre_o}\n📍 {dir_o}\n🌍 {fm}\n👷 Encargado: {enc['nombre']}\n\n📁 Carpeta Drive + Sheet creados"
            except Exception as e:
                log.error(f"WF-15 error: {e}")
                s.respuesta="Error creando la obra. Inténtalo de nuevo."
            s.dominio="OBRA_ALTA"; s.dominio_fuente="espera"
            s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
        # OBRA BAJA
        if esp.get("tipo")=="obra_baja":
            ctx=esp.get("contexto",{}) or {}
            obras=await db_get("obras","select=id,nombre&estado=eq.En curso&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1
                obra=obras[sel] if 0<=sel<len(obras) else None
            except: obra=None
            if obra:
                async with httpx.AsyncClient(timeout=15) as pc:
                    await pc.patch(f"{SUPA}/rest/v1/obras?id=eq.{obra['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json={"estado":"Cerrada"})
                s.respuesta=f"\u2705 Obra *{obra['nombre']}* cerrada. Ya no aparecerá en fichajes ni facturas."
            else:
                s.respuesta="No encontré esa obra. Dime el número."
            s.dominio="OBRA_BAJA"; s.dominio_fuente="espera"
            s.duracion_ms=int((time.time()-t0)*1000); await guardar_ejecucion(s); return s
        if esp.get("tipo")=="n8n_pending":
            log.info(f"[{s.trace_id}] Forwarding text to n8n (espera n8n_pending)")
            try:
                fwd={"data":{"key":{"remoteJid":f"{s.telefono}@s.whatsapp.net","fromMe":False},"message":{"conversation":s.mensaje_original}}}
                async with httpx.AsyncClient(timeout=30) as fc: await fc.post(N8N_WEBHOOK,json=fwd)
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":0,"tipo":"n8n_pending","dominio":"N8N","contexto":{"text":True}})
            except Exception as e: log.error(f"Forward to n8n: {e}")
            s.dominio="N8N"; s.dominio_fuente="espera"; s.respuesta=""; s.duracion_ms=int((time.time()-t0)*1000)
            await guardar_ejecucion(s); return s
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
        msg_key=msg.get("key",{})
        # Detect message type
        m=msg.get("message",{})
        cont=m.get("conversation","") or m.get("extendedTextMessage",{}).get("text","")
        has_image=bool(m.get("imageMessage"))
        has_audio=bool(m.get("audioMessage"))
        has_doc=bool(m.get("documentMessage"))
        # Forward non-text to n8n WF-1
        if has_image:
            log.info(f"Image received from {tel}")
            emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
            emp=emps[0] if emps else {"id":0,"nombre":"?","telefono":tel}
            # Download image from Evolution
            img_msg=m.get("imageMessage",{})
            caption=img_msg.get("caption","") or m.get("conversation","")
            msg_key=msg.get("key",{})
            try:
                import base64
                # Use Evolution API to get decrypted base64
                img_b64=""
                async with httpx.AsyncClient(timeout=30) as dl:
                    evo_r=await dl.post(f"{EVO}/chat/getBase64FromMediaMessage/{INSTANCE}",
                        headers={"apikey":EK,"Content-Type":"application/json"},
                        json={"message":{"key":msg_key},"convertToMp4":False})
                    if evo_r.status_code==200 or evo_r.status_code==201:
                        evo_data=evo_r.json()
                        img_b64=evo_data.get("base64","")
                        log.info(f"Evolution base64: {len(img_b64)} chars")
                    else:
                        log.error(f"Evolution media: {evo_r.status_code} {evo_r.text[:200]}")
                # OCR with GPT-4o Vision
                if img_b64:
                    ocr_prompt="Analiza esta factura/ticket. Extrae: proveedor, CIF, fecha (YYYY-MM-DD), concepto, base_imponible, iva_porcentaje, iva_importe, total. Responde SOLO JSON sin markdown."
                    mime=img_msg.get("mimetype","image/jpeg")
                    ocr_msgs=[{"role":"user","content":[{"type":"text","text":ocr_prompt},{"type":"image_url","image_url":{"url":f"data:{mime};base64,{img_b64}"}}]}]
                    async with httpx.AsyncClient(timeout=60) as oc:
                        ocr_r=await oc.post("https://api.openai.com/v1/chat/completions",headers={"Authorization":f"Bearer {OPENAI_KEY}","Content-Type":"application/json"},
                            json={"model":"gpt-4o-mini","messages":ocr_msgs,"max_tokens":500})
                        ocr_data=ocr_r.json()
                        log.info(f"OCR status: {'ok' if 'choices' in ocr_data else ocr_data.get('error',{}).get('message','?')}")
                        if "error" in ocr_data:
                            log.error(f"OpenAI Vision error: {ocr_data.get('error')}")
                            ocr_raw='{"proveedor":"No pude leer","total":0}'
                        else:
                            ocr_raw=ocr_data["choices"][0]["message"]["content"]
                    if "```" in ocr_raw: ocr_raw=ocr_raw.split("```")[1].replace("json","").strip()
                    try: factura_data=json.loads(ocr_raw)
                    except: factura_data={"proveedor":"?","total":0,"fecha":"?"}
                else:
                    factura_data={"proveedor":"Desconocido","total":0}
                # Get obras for selection
                obras=await db_get("obras","select=id,nombre&estado=eq.En curso&order=nombre")
                obras_txt="\n".join([f"{i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
                prov=factura_data.get("proveedor","?")
                total=factura_data.get("total",0)
                fecha=factura_data.get("fecha","?")
                resp=f"He leido esta factura:\n\nProveedor: {prov}\nFecha: {fecha}\nTotal: {total}EUR\n\n¿A qué obra va? 🏗️\n\n{obras_txt}\n\nDime número o nombre 😊"
                await wa(f"{tel}@s.whatsapp.net",resp)
                # Save espera with factura data
                await db_post("bia_esperas",{"telefono":tel,"empleado_id":emp.get("id",0),"tipo":"factura_obra","dominio":"FACTURA","contexto":{"factura":factura_data,"obras":[o["id"] for o in obras]}})
                await db_post("bia_ejecuciones",{"trace_id":str(uuid.uuid4())[:8],"telefono":tel,"empleado_id":emp.get("id"),"empleado_nombre":emp.get("nombre",""),"input_original":"[imagen]","dominio":"FACTURA","dominio_fuente":"media","estado_final":"ok","respuesta":resp[:500],"duracion_ms":0})
            except Exception as e:
                log.error(f"Image processing error: {e}")
                await wa(f"{tel}@s.whatsapp.net",f"No pude leer la factura. ¿Puedes enviarla más clara? 🔧")
            return {"ok":True}
        if has_audio:
            log.info(f"Audio received from {tel}")
            emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
            if not emps:
                await wa(f"{tel}@s.whatsapp.net","No estás registrado.")
                return {"ok":True}
            emp=emps[0]
            try:
                # Download audio via Evolution API
                audio_b64=""
                async with httpx.AsyncClient(timeout=30) as dl:
                    evo_r=await dl.post(f"{EVO}/chat/getBase64FromMediaMessage/{INSTANCE}",
                        headers={"apikey":EK,"Content-Type":"application/json"},
                        json={"message":{"key":msg_key},"convertToMp4":False})
                    if evo_r.status_code in(200,201):
                        evo_data=evo_r.json()
                        audio_b64=evo_data.get("base64","")
                        log.info(f"Audio base64: {len(audio_b64)} chars")
                if audio_b64:
                    import base64 as b64mod
                    audio_bytes=b64mod.b64decode(audio_b64)
                    # Transcribe with Whisper
                    import io
                    files={"file":("audio.ogg",io.BytesIO(audio_bytes),"audio/ogg")}
                    async with httpx.AsyncClient(timeout=30) as wc:
                        wr=await wc.post("https://api.openai.com/v1/audio/transcriptions",
                            headers={"Authorization":f"Bearer {OPENAI_KEY}"},
                            data={"model":"whisper-1"},files=files)
                        transcription=wr.json().get("text","")
                    log.info(f"Whisper: {transcription[:80]}")
                    if transcription:
                        # Process transcribed text through normal router
                        s=BiaState(telefono=tel,mensaje_original=transcription,tipo_mensaje="audio",empleado=emp)
                        s=await procesar(s)
                        if s.respuesta: await wa(f"{tel}@s.whatsapp.net",s.respuesta)
                        return {"ok":True,"trace_id":s.trace_id}
                    else:
                        await wa(f"{tel}@s.whatsapp.net","No pude entender el audio. ¿Puedes escribirlo? 🎤")
                else:
                    await wa(f"{tel}@s.whatsapp.net","No pude descargar el audio. Intenta de nuevo 🔧")
            except Exception as e:
                log.error(f"Audio error: {e}")
                await wa(f"{tel}@s.whatsapp.net","Problema con el audio. ¿Puedes escribirlo? 🎤")
            return {"ok":True}
        if has_doc:
            log.info(f"Forwarding doc to n8n")
            try:
                async with httpx.AsyncClient(timeout=30) as fc: await fc.post(N8N_WEBHOOK,json=data)
            except Exception as e: log.error(f"Forward: {e}")
            return {"ok":True,"forwarded":"n8n"}
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
async def health(): return {"status":"ok","service":"bia-v3","version":"4.5-fix"}

if __name__=="__main__":
    import uvicorn; uvicorn.run(app,host="0.0.0.0",port=PORT)
