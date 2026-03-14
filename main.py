#!/usr/bin/env python3
"""BIA v7.6 — Fichajes Blindados + Parser v2 + Personalidad + Memoria + Facturas"""
import os, re, json, time, uuid, logging, hashlib
from dataclasses import dataclass, field
from datetime import datetime, date
from fastapi import FastAPI, Request
import httpx

SUPA=os.getenv("SUPABASE_URL",""); SK=os.getenv("SUPABASE_KEY","")
EVO=os.getenv("EVOLUTION_URL",""); EK=os.getenv("EVOLUTION_KEY","")
INSTANCE=os.getenv("EVOLUTION_INSTANCE","EuromirBia")
PYTHON_URL=os.getenv("PYTHON_FICHAJES_URL",""); N8N=os.getenv("N8N_URL","")
N8N_WEBHOOK=os.getenv("N8N_WEBHOOK","")
OPENAI_KEY=os.getenv("OPENAI_API_KEY",""); PORT=int(os.getenv("PORT","8001"))
GOTENBERG=os.getenv("GOTENBERG_URL","https://gotenberg-gotenberg.wp2z39.easypanel.host")
DRIVE_FOLDERS={"T1":"1J0speoBjoBQU_t5sjKacuSCMLAwYQbW9","T2":"1kc_YYAY5q-M18qFcXwDQ0dTy02jScmGf","T3":"1Zk4GAIcRus5z7D27gmBmi2F_bK8AdJWo","T4":"1f8a1au4AFPgoGnJXs7SMfg96irTVLt-m"}
LLM_CONF_HIGH=0.80  # >= 0.80: use directly
LLM_CONF_MEDIUM=0.50  # 0.50-0.79: ask clarification
# < 0.50: reject as not fichaje

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log=logging.getLogger("bia-v3")
app=FastAPI(title="BIA v7",version="7.1")
ESPERA_TTL_MINUTES=20

@dataclass
class BiaState:
    trace_id:str="";telefono:str="";mensaje_original:str="";mensaje_normalizado:str=""
    tipo_mensaje:str="texto";empleado:dict=field(default_factory=dict);historial:str=""
    dominio:str="";dominio_fuente:str="";accion:str="";respuesta:str=""
    confianza:float=1.0;necesita_humano:bool=False;errores:list=field(default_factory=list)
    metadata:dict=field(default_factory=dict);timestamps:dict=field(default_factory=dict);duracion_ms:int=0
    def timer_start(self,s):self.timestamps[f"{s}_start"]=time.time()
    def timer_end(self,s):self.timestamps[f"{s}_ms"]=int((time.time()-self.timestamps.get(f"{s}_start",time.time()))*1000)
    def add_error(self,e,rec=True):self.errores.append({"error":e});log.error(f"[{self.trace_id}] {e}")

# ══════════════ HELPERS ══════════════
async def db_get(t,q=""):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.get(f"{SUPA}/rest/v1/{t}?{q}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"})
            return safe_json_response(r,f"db_get:{t}",[]) if r.status_code==200 else []
    except: return []

async def db_post(t,d):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.post(f"{SUPA}/rest/v1/{t}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json=d)
            return safe_json_response(r,f"db_post:{t}",{"error":"invalid json"}) if r.status_code in(200,201) else {"error":r.text}
    except Exception as e: return {"error":str(e)}

async def db_upsert(t,d,on="telefono"):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.post(
                f"{SUPA}/rest/v1/{t}?on_conflict={on}",
                headers={
                    "apikey":SK,
                    "Authorization":f"Bearer {SK}",
                    "Content-Type":"application/json",
                    "Prefer":"resolution=merge-duplicates,return=representation"
                },
                json=d
            )
            return safe_json_response(r,f"db_upsert:{t}",{"error":"invalid json"}) if r.status_code in(200,201) else {"error":r.text}
    except Exception as e:
        return {"error":str(e)}

def safe_json_response(resp,where="",default=None):
    try:
        return resp.json()
    except Exception as e:
        raw=(getattr(resp,"text","") or "")[:300].replace("\n"," ").replace("\r"," ")
        ct=(getattr(resp,"headers",{}) or {}).get("content-type","")
        status=getattr(resp,"status_code","?")
        log.error(f"{where} JSON invalido status={status} ct={ct} body={raw}")
        if isinstance(default,dict):
            out=dict(default)
            out.setdefault("error",f"{where} invalid json")
            out["_parse_error"]=str(e)
            out["_http_status"]=status
            out["_raw_text"]=raw
            return out
        return default if default is not None else {}

async def guardar_ejecucion(s):
    await db_post("bia_ejecuciones",{"trace_id":s.trace_id,"telefono":s.telefono,"empleado_id":s.empleado.get("id"),"empleado_nombre":s.empleado.get("nombre",""),"input_original":(s.mensaje_original or "")[:2000],"input_normalizado":(s.mensaje_normalizado or "")[:2000],"tipo_mensaje":s.tipo_mensaje,"dominio":s.dominio,"dominio_fuente":s.dominio_fuente,"accion":s.accion,"confianza":s.confianza,"agente":s.dominio,"estado_final":"error" if s.errores else "ok","respuesta":(s.respuesta or "")[:2000],"necesita_humano":s.necesita_humano,"error":json.dumps(s.errores) if s.errores else None,"duracion_ms":s.duracion_ms,"metadata":json.dumps({"timestamps":s.timestamps})})

async def wa(num,txt):
    try:
        async with httpx.AsyncClient(timeout=15) as c: await c.post(f"{EVO}/message/sendText/{INSTANCE}",headers={"apikey":EK,"Content-Type":"application/json"},json={"number":num,"text":txt})
    except Exception as e: log.error(f"WA: {e}")

async def gpt(prompt,system="",model="gpt-4o-mini",max_t=500):
    try:
        msgs=[];
        if system: msgs.append({"role":"system","content":system})
        msgs.append({"role":"user","content":prompt})
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post("https://api.openai.com/v1/chat/completions",headers={"Authorization":f"Bearer {OPENAI_KEY}","Content-Type":"application/json"},json={"model":model,"messages":msgs,"max_tokens":max_t,"temperature":0.3})
            d=safe_json_response(r,"gpt",{})
            return d.get("choices",[{"message":{"content":""}}])[0]["message"]["content"]
    except Exception as e: log.error(f"GPT: {e}"); return ""

async def cargar_historial(tel,limit=20):
    msgs=await db_get("bia_chat_history",f"telefono=eq.{tel}&order=created_at.desc&limit={limit}&select=role,content")
    return list(reversed(msgs))

async def guardar_msg(tel,eid,role,content):
    await db_post("bia_chat_history",{"telefono":tel,"empleado_id":eid,"role":role,"content":(content or "")[:1000]})

def _ctx_value(v):
    if v is None:
        return None
    if isinstance(v,str):
        v=v.strip()
        return v or None
    return v

def _ctx_mes_txt(mes,anio=None):
    try:
        mes_i=int(mes)
    except:
        return _ctx_value(mes)
    if mes_i<1 or mes_i>12:
        return None
    nombre=["","enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"][mes_i]
    return f"{nombre} {anio}" if anio else nombre

_CTX_MONTHS={
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
    "ianuarie":1,"februarie":2,"martie":3,"aprilie":4,"mai":5,"iunie":6,
    "iulie":7,"august":8,"septembrie":9,"octombrie":10,"noiembrie":11,"decembrie":12
}

def _ctx_parse_mes(valor,anio_hint=None):
    valor=_ctx_value(valor)
    if valor is None:
        return None,None,None
    if isinstance(valor,int):
        return (valor if 1<=valor<=12 else None), anio_hint, _ctx_mes_txt(valor,anio_hint)
    txt=str(valor).strip().lower()
    if txt.isdigit():
        mi=int(txt)
        return (mi if 1<=mi<=12 else None), anio_hint, _ctx_mes_txt(mi,anio_hint)
    m=re.match(r"^(\d{1,2})[/-](\d{4})$",txt)
    if m:
        mi=int(m.group(1)); ay=int(m.group(2))
        return (mi if 1<=mi<=12 else None), ay, _ctx_mes_txt(mi,ay)
    m=re.match(r"^([a-záéíóúñ]+)\s+(\d{4})$",txt,re.I)
    if m:
        mi=_CTX_MONTHS.get(m.group(1).lower()); ay=int(m.group(2))
        return mi, ay, _ctx_mes_txt(mi,ay) if mi else valor
    mi=_CTX_MONTHS.get(txt)
    if mi:
        return mi, anio_hint, _ctx_mes_txt(mi,anio_hint)
    return None, anio_hint, valor

def _ctx_from_row(row):
    row=row or {}
    meta=row.get("metadata") or {}
    mes_txt=meta.get("mes_txt") or _ctx_mes_txt(row.get("mes"),row.get("anio"))
    ctx={
        "empleado_id":row.get("empleado_id"),
        "tema":row.get("tema"),
        "subtema":row.get("subtema"),
        "paso":row.get("paso"),
        "estado_flujo":row.get("estado_flujo"),
        "dominio":row.get("dominio"),
        "obra_id":row.get("obra_id"),
        "obra":row.get("obra_nombre"),
        "empleado_objetivo_id":row.get("empleado_objetivo_id"),
        "empleado":row.get("empleado_objetivo_nombre"),
        "mes":mes_txt,
        "anio":row.get("anio"),
        "fecha":row.get("fecha"),
        "ultima_pregunta":row.get("ultima_pregunta"),
        "ultimo_mensaje_user":row.get("ultimo_mensaje_user"),
        "ultima_respuesta_bia":row.get("ultima_respuesta_bia"),
        "updated_at":row.get("updated_at")
    }
    for k,v in meta.items():
        if k not in ("mes_txt",) and k not in ctx:
            ctx[k]=v
    return {k:v for k,v in ctx.items() if v not in (None,"",{},[])}

def _tema_base_desde_dominio(dominio):
    dom=(dominio or "").upper()
    base={
        "FICHAJE":"fichaje",
        "NOMINA":"nomina",
        "FINANZAS":"finanzas",
        "OBRAS":"obras",
        "EMPLEADOS":"empleados",
        "FACTURA":"factura",
        "OBRA_ALTA":"obra_alta",
        "OBRA_BAJA":"obra_baja",
        "VACACIONES":"ausencias",
        "AUSENCIAS":"ausencias"
    }
    return base.get(dom)

def _tema_compatible_con_dominio(tema,dominio):
    if not tema or not dominio:
        return True
    dom=(dominio or "").upper()
    if dom in ("GENERAL","SALUDO","MENU","INTENT","AMBIGUO"):
        return True
    tema_l=str(tema).lower()
    allowed={
        "FICHAJE":{"fichaje","horas_obra"},
        "NOMINA":{"nomina","anticipo"},
        "FINANZAS":{"finanzas","gastos_obra","gastos_empleado","coste_obra","anticipo"},
        "OBRAS":{"obras","obra_alta","obra_baja","reabrir_obra","encargado"},
        "EMPLEADOS":{"empleados","encargado"},
        "FACTURA":{"factura","gastos_obra"},
        "VACACIONES":{"ausencias"},
        "AUSENCIAS":{"ausencias"}
    }.get(dom)
    if not allowed:
        return True
    return tema_l in allowed

def _tema_entity_policy(tema):
    tema_l=(tema or "").lower()
    if tema_l in ("gastos_obra","horas_obra","fichaje","obras","obra_alta","obra_baja","reabrir_obra","encargado","coste_obra"):
        return {"obra":True,"empleado":tema_l=="encargado","mes":tema_l in ("gastos_obra","horas_obra","coste_obra"),"fecha":tema_l=="fichaje"}
    if tema_l in ("gastos_empleado","nomina","anticipo","empleados"):
        return {"obra":False,"empleado":True,"mes":tema_l in ("gastos_empleado","nomina","anticipo"),"fecha":False}
    if tema_l in ("finanzas","factura"):
        return {"obra":tema_l=="factura","empleado":False,"mes":False,"fecha":False}
    return {"obra":False,"empleado":False,"mes":False,"fecha":False}

def _limpiar_ctx_por_tema(payload,meta,tema_nuevo,tema_anterior):
    if not tema_nuevo:
        return False
    if tema_anterior and tema_nuevo.lower()==str(tema_anterior).lower():
        return False
    pol=_tema_entity_policy(tema_nuevo)
    changed=False
    if not pol.get("obra"):
        if payload.get("obra_id") is not None:
            payload["obra_id"]=None; changed=True
        if payload.get("obra_nombre") is not None:
            payload["obra_nombre"]=None; changed=True
    if not pol.get("empleado"):
        if payload.get("empleado_objetivo_id") is not None:
            payload["empleado_objetivo_id"]=None; changed=True
        if payload.get("empleado_objetivo_nombre") is not None:
            payload["empleado_objetivo_nombre"]=None; changed=True
    if not pol.get("mes"):
        if payload.get("mes") is not None:
            payload["mes"]=None; changed=True
        if payload.get("anio") is not None:
            payload["anio"]=None; changed=True
        if meta.get("mes_txt") is not None:
            meta.pop("mes_txt",None); changed=True
    if not pol.get("fecha"):
        if payload.get("fecha") is not None:
            payload["fecha"]=None; changed=True
    return changed

_RGX_SALUDO=re.compile(r'^(hola|holi|buenas(?:\s+d[ií]as|\s+tardes|\s+noches)?|hey|que tal|qué tal|ola)[\s!.,?]*$',re.I)
_RGX_CORTESIA=re.compile(r'^(gracias|muchas gracias|ok|vale|perfecto|genial|estupendo|entendido|de acuerdo|oki|dale|bien)[\s!.,?]*$',re.I)
_RGX_SALUDO_APERTURA=re.compile(r'^\s*(hola|buenas|hey|qué tal|que tal|gabriel[,!]?|hola[,!]\s+\w+)',re.I)

def es_saludo_simple(txt):
    return bool(_RGX_SALUDO.match((txt or "").strip()))

def es_cortesia_simple(txt):
    return bool(_RGX_CORTESIA.match((txt or "").strip()))

def debe_saludar(msg_user,hist):
    if es_saludo_simple(msg_user):
        return True
    recientes=list(hist or [])[-4:]
    if not recientes:
        return True
    for m in reversed(recientes):
        if m.get("role")=="assistant" and _RGX_SALUDO_APERTURA.search(m.get("content","")):
            return False
    return False

def quitar_saludo_repetido(txt):
    if not txt:
        return txt
    t=txt.strip()
    t=re.sub(r'^\s*(hola|holi|buenas(?:\s+d[ií]as|\s+tardes|\s+noches)?|hey)[^,\n.!?]*[,!.\s]*', '', t, count=1, flags=re.I)
    t=re.sub(r'^\s*gabriel[,!.\s]*', '', t, count=1, flags=re.I)
    t=t.lstrip()
    return t or txt.strip()

def _norm_txt(s):
    import unicodedata
    s=''.join(c for c in unicodedata.normalize('NFD',str(s or "")) if unicodedata.category(c)!='Mn')
    return re.sub(r'[^a-z0-9\s]',' ',s.lower()).strip()

def es_followup_empleado_ctx(txt,ctx):
    emp=_norm_txt((ctx or {}).get("empleado",""))
    if not emp:
        return False
    t=_norm_txt(txt)
    if not t or len(t)>40:
        return False
    emp_tokens=[x for x in emp.split() if x]
    if not emp_tokens:
        return False
    emp_first=emp_tokens[0]
    variantes={emp,emp_first,f"de {emp}",f"de {emp_first}",f"y {emp_first}",f"y {emp}"}
    return t in variantes

async def cargar_contexto_resumido(tel):
    rows=await db_get("bia_contexto_activo",f"telefono=eq.{tel}&select=telefono,empleado_id,tema,subtema,paso,estado_flujo,dominio,obra_id,obra_nombre,empleado_objetivo_id,empleado_objetivo_nombre,mes,anio,fecha,ultima_pregunta,ultimo_mensaje_user,ultima_respuesta_bia,metadata,updated_at&limit=1")
    if not rows:
        return {}
    return _ctx_from_row(rows[0])

async def guardar_contexto_resumido(tel,eid,**updates):
    rows=await db_get("bia_contexto_activo",f"telefono=eq.{tel}&select=*&limit=1")
    row=(rows[0] if rows else {}) or {}
    meta=dict(row.get("metadata") or {})
    changed=False
    tema_anterior=row.get("tema")
    payload={
        "telefono":tel,
        "empleado_id":eid or row.get("empleado_id"),
        "tema":row.get("tema"),
        "subtema":row.get("subtema"),
        "paso":row.get("paso"),
        "estado_flujo":row.get("estado_flujo"),
        "dominio":row.get("dominio"),
        "obra_id":row.get("obra_id"),
        "obra_nombre":row.get("obra_nombre"),
        "empleado_objetivo_id":row.get("empleado_objetivo_id"),
        "empleado_objetivo_nombre":row.get("empleado_objetivo_nombre"),
        "mes":row.get("mes"),
        "anio":row.get("anio"),
        "fecha":row.get("fecha"),
        "ultima_pregunta":row.get("ultima_pregunta"),
        "ultimo_mensaje_user":row.get("ultimo_mensaje_user"),
        "ultima_respuesta_bia":row.get("ultima_respuesta_bia"),
        "confianza_contexto":row.get("confianza_contexto") or 1.0,
        "metadata":meta
    }
    alias_map={
        "obra":"obra_nombre",
        "empleado":"empleado_objetivo_nombre"
    }
    tema_nuevo=_ctx_value(updates.get("tema")) or payload.get("tema")
    if _limpiar_ctx_por_tema(payload,meta,tema_nuevo,tema_anterior):
        changed=True
    for k,v in updates.items():
        key=alias_map.get(k,k)
        vv=_ctx_value(v)
        if vv is None:
            continue
        if key=="mes":
            mi,ay,mes_txt=_ctx_parse_mes(vv,updates.get("anio") or payload.get("anio"))
            if mi and payload.get("mes")!=mi:
                payload["mes"]=mi; changed=True
            if ay and payload.get("anio")!=ay:
                payload["anio"]=ay; changed=True
            if mes_txt and meta.get("mes_txt")!=mes_txt:
                meta["mes_txt"]=mes_txt; changed=True
            continue
        if key in payload:
            if payload.get(key)!=vv:
                payload[key]=vv
                changed=True
        else:
            if meta.get(key)!=vv:
                meta[key]=vv
                changed=True
    if not changed:
        return _ctx_from_row(payload)
    payload["metadata"]=meta
    payload["updated_at"]=datetime.now().isoformat(timespec="seconds")
    await db_upsert("bia_contexto_activo",payload,"telefono")
    return _ctx_from_row(payload)

async def cargar_memoria_hechos(tel,limit=8):
    rows=await db_get("bia_memoria_vigente",f"telefono=eq.{tel}&order=relevancia.desc,updated_at.desc&limit={limit}&select=tema,clave,valor,valor_json,relevancia,updated_at")
    out=[]
    for row in rows or []:
        valor=_ctx_value(row.get("valor"))
        if not valor:
            vj=row.get("valor_json") or {}
            valor=", ".join([f"{k}: {v}" for k,v in vj.items()][:3]) if isinstance(vj,dict) else None
        if valor:
            out.append({"tema":row.get("tema"),"clave":row.get("clave"),"valor":valor,"relevancia":row.get("relevancia",0),"updated_at":row.get("updated_at")})
    return out

async def guardar_memoria_hecho(tel,eid,tema,clave,valor=None,valor_json=None,relevancia=50,fuente="bia",vigente=True,caduca_en=None):
    if not tel or not tema or not clave:
        return {}
    payload={
        "telefono":tel,
        "empleado_id":eid or None,
        "tema":tema,
        "clave":clave,
        "valor":_ctx_value(valor),
        "valor_json":valor_json if isinstance(valor_json,dict) else {},
        "fuente":fuente,
        "relevancia":relevancia,
        "vigente":vigente,
        "caduca_en":caduca_en
    }
    return await db_upsert("bia_memoria_hechos",payload,"telefono,tema,clave")

async def cargar_resumenes_dialogo(tel,limit=3):
    rows=await db_get("bia_resumenes_dialogo",f"telefono=eq.{tel}&order=created_at.desc&limit={limit}&select=tema,resumen,entidades,created_at")
    return rows or []

async def guardar_resumen_dialogo(tel,eid,resumen,tema="",entidades=None,desde=None,hasta=None,mensajes_cubiertos=1):
    resumen=(resumen or "").strip()
    if not tel or not resumen:
        return {}
    return await db_post("bia_resumenes_dialogo",{
        "telefono":tel,
        "empleado_id":eid or None,
        "tema":tema or None,
        "resumen":resumen[:1000],
        "entidades":entidades if isinstance(entidades,dict) else {},
        "desde":desde,
        "hasta":hasta,
        "mensajes_cubiertos":mensajes_cubiertos
    })

async def registrar_memoria_turno(s):
    try:
        eid=s.empleado.get("id",0)
        es_cortesia=es_cortesia_simple(s.mensaje_normalizado)
        ctx_antes=await cargar_contexto_resumido(s.telefono)
        await guardar_contexto_resumido(
            s.telefono,eid,
            dominio=s.dominio,
            ultimo_mensaje_user=s.mensaje_normalizado,
            ultima_respuesta_bia=s.respuesta
        )
        idioma=_ctx_value(s.empleado.get("idioma"))
        if idioma:
            await guardar_memoria_hecho(s.telefono,eid,"preferencia","idioma",idioma,relevancia=90,fuente="empleado")
        tono="directo" if s.empleado.get("notas_bia") else None
        if tono:
            await guardar_memoria_hecho(s.telefono,eid,"preferencia","tono",tono,relevancia=60,fuente="empleado")
        ctx=await cargar_contexto_resumido(s.telefono)
        tema_ctx=ctx.get("tema")
        tema_mem=tema_ctx
        if es_cortesia and ctx_antes.get("tema"):
            tema_mem=ctx_antes.get("tema")
        coherente=_tema_compatible_con_dominio(tema_ctx,s.dominio)
        if not coherente:
            tema_mem=_tema_base_desde_dominio(s.dominio) or tema_ctx
        entidades={}
        if tema_mem:
            await guardar_memoria_hecho(s.telefono,eid,"contexto","ultimo_tema",tema_mem,relevancia=35,fuente="contexto")
        if coherente:
            for k in ("obra","empleado","mes","fecha"):
                if ctx.get(k):
                    entidades[k]=ctx.get(k)
            if ctx.get("obra"):
                await guardar_memoria_hecho(s.telefono,eid,"referencia","ultima_obra",ctx.get("obra"),relevancia=45,fuente="contexto")
            if ctx.get("empleado"):
                await guardar_memoria_hecho(s.telefono,eid,"referencia","ultimo_empleado",ctx.get("empleado"),relevancia=45,fuente="contexto")
            if ctx.get("mes"):
                await guardar_memoria_hecho(s.telefono,eid,"referencia","ultimo_mes",ctx.get("mes"),relevancia=35,fuente="contexto")
        elif ctx_antes.get("tema") and ctx_antes.get("tema")!=tema_mem:
            log.info(f"[{s.trace_id}] memoria conservadora: no arrastro entidades de {ctx_antes.get('tema')} a {s.dominio}")
        if s.respuesta and s.dominio not in ("SALUDO","GENERAL") and not es_cortesia:
            resumen=f"Usuario: {s.mensaje_normalizado[:180]}. Bia: {s.respuesta[:220]}"
            await guardar_resumen_dialogo(s.telefono,eid,resumen,tema=tema_mem or s.dominio.lower(),entidades=entidades,hasta=datetime.now().isoformat(timespec='seconds'))
    except Exception as e:
        log.error(f"registrar_memoria_turno: {e}")

async def borrar_espera(espera_id):
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.delete(f"{SUPA}/rest/v1/bia_esperas?id=eq.{espera_id}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"})
    except Exception as e:
        log.error(f"Borrar espera: {e}")

def espera_caducada(esp,ttl_min=ESPERA_TTL_MINUTES):
    try:
        ct=(esp or {}).get("created_at","")
        if not ct:
            return False
        created=datetime.fromisoformat(ct.replace("Z","+00:00"))
        ahora=datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
        return (ahora-created)>timedelta(minutes=ttl_min)
    except Exception:
        return False

# ══════════════ PERSONALIDAD ══════════════
BIA_PERSONA="Eres Bia, secretaria inteligente de Euromir. Cercana, directa, con chispa y humor. Emojis con naturalidad. CORTO para WhatsApp (3-5 lineas). Adapta el tono segun las notas del empleado. No saludes ni digas 'hola' en cada respuesta: si la conversacion ya esta en marcha, continua natural y directo."

# ══════════════ PARSER FICHAJES v2.0 ══════════════
FP=[
    {"n":"dash","r":re.compile(r'(\d{1,2})[:.h]?(\d{2})?\s*[-\u2013]\s*(\d{1,2})[:.h]?(\d{2})?')},
    {"n":"de_a","r":re.compile(r'de\s+(\d{1,2})[:.h]?(\d{2})?\s+(?:a|h?asta)\s+(?:las?\s+)?(\d{1,2})[:.h]?(\d{2})?',re.I)},
    {"n":"a","r":re.compile(r'(\d{1,2})[:.h]?(\d{2})?\s+(?:a|h?asta)\s+(?:las?\s+)?(\d{1,2})[:.h]?(\d{2})?',re.I)},
    {"n":"la","r":re.compile(r'(\d{1,2})[:.h]?(\d{2})?\s+la\s+(\d{1,2})[:.h]?(\d{2})?',re.I)},
    {"n":"es","r":re.compile(r'(?:entrad[oa]|entrada|empezad[oa]|empiezo)\s+(?:a\s+las?\s+)?(\d{1,2})[:.h]?(\d{2})?.+(?:salid[oa]|salida|terminad[oa]|termino|acabado)\s+(?:a\s+las?\s+)?(\d{1,2})[:.h]?(\d{2})?',re.I)},
    {"n":"ys","r":re.compile(r'(\d{1,2})[:.h]?(\d{2})?\s+(?:y|e)\s+(?:salgo|termino|acabo)\s+(?:a\s+las?\s+)?(\d{1,2})[:.h]?(\d{2})?',re.I)},
    {"n":"hf","r":re.compile(r'(\d{1,2})h\s*[-\u2013a]\s*(\d{1,2})h',re.I)},
    {"n":"sl","r":re.compile(r'(\d{1,2})[:.h]?(\d{2})?\s*/\s*(\d{1,2})[:.h]?(\d{2})?')},
    {"n":"hr","r":re.compile(r'(\d{1,2})\s*horas?\b',re.I)},
]
FK=re.compile(r'trabaj|ficha|jornada|turno|empiezo|salgo|entrad|salid|curr|hice|estuve|lucrat',re.I)
FQ=re.compile(r'cuantos|cuántos|quién|quien|empleados.+fich|han fichado|ficharon.+hoy|resumen|informe',re.I)
FK2=re.compile(r'\d+\s+y\s+(media|cuarto|pico)|ma[nñ]ana\s+\d+\s.+hasta|\d+\s.+hasta\s+las',re.I)
# Anti-patterns: messages that look like fichaje but aren't
FA=re.compile(r'(?:(?:ayer|anteayer|antes).+(?:hoy|ahora)|(?:hoy|ahora).+(?:ma[nñ]ana|luego|despu[eé]s)|(?:pedro|juan|carlos|miguel|otro|alguien|[eé]l|ella)\s.+\d{1,2}|no\s+s[eé]|\d{1,2}\s+\d{1,2}\s+\d{1,2}|trabajamos|hicimos|estuvimos|hicieron|y\s+(?:luego|despu[eé]s|m[aá]s\s+tarde)\s+\d)',re.I)

def has_multiple_ranges(texto):
    """Detect 2+ time ranges in message (multi-turno)"""
    t=texto.lower()
    r1=re.findall(r'\d{1,2}[:.h]?\d{0,2}\s*[-\u2013]\s*\d{1,2}[:.h]?\d{0,2}',t)
    r2=re.findall(r'de\s+\d{1,2}\s+(?:a|hasta)\s+\d{1,2}',t)
    return len(r1)+len(r2)>=2

def nt(v):
    if not v: return None
    v=str(v).strip().lower().replace('h','')
    m=re.match(r'^(\d{1,2})[:.h]?(\d{0,2})$',v)
    if not m: return None
    h,mn=int(m.group(1)),int(m.group(2)) if m.group(2) else 0
    return f"{h:02d}:{mn:02d}" if 0<=h<=23 and 0<=mn<=59 else None

def normalizar_turno(sr,er):
    """Normalize shift with 5 clear cases: NORMAL, INFERRED_PM, OVERNIGHT, AMBIGUOUS, INVALID"""
    s,e=nt(sr),nt(er)
    if not s or not e: return {"ok":False,"error_code":"PARSE_ERROR","ambiguous":False}
    sh,sm=map(int,s.split(':'));eh,em=map(int,e.split(':'))
    s_min,e_min=sh*60+sm,eh*60+em
    overnight,inferred_pm=False,False
    if e_min>s_min:
        dur=e_min-s_min
    elif e_min==s_min:
        return {"ok":False,"error_code":"ZERO_DURATION","ambiguous":False}
    else:
        gap=sh-eh
        if 5<=sh<=12 and 1<=eh<=9 and gap>=2:
            eh+=12;e=f"{eh:02d}:{em:02d}";e_min=eh*60+em;dur=e_min-s_min;inferred_pm=True
        elif sh>=18 and eh<12:
            dur=(24*60-s_min)+e_min;overnight=True
        elif sh>=14 and eh<=8:
            dur=(24*60-s_min)+e_min;overnight=True
        else:
            return {"ok":False,"entrada":s,"salida":f"{eh:02d}:{em:02d}","error_code":"AMBIGUOUS_SHIFT","ambiguous":True,"inferred_pm":False,"overnight":False}
    dh=round(dur/60,1)
    if dh<=0: return {"ok":False,"error_code":"ZERO_DURATION","ambiguous":False}
    if dh>18: return {"ok":False,"error_code":"EXCESSIVE_DURATION","ambiguous":False,"dur":dh}
    return {"ok":True,"entrada":s,"salida":e,"overnight":overnight,"inferred_pm":inferred_pm,"ambiguous":False,"dur":dh,"error_code":None}

def parse_fichaje(texto):
    t=texto.lower().strip()
    # Check anti-patterns first
    if FA.search(t) or has_multiple_ranges(t): return {"det":True,"pat":"anti","needs_llm":True,"anti_pattern":True}
    for p in FP:
        m=p["r"].search(t)
        if m:
            g=m.groups()
            if p["n"]=="hr": return {"det":True,"pat":p["n"],"solo_h":int(g[0]),"needs_times":True}
            if p["n"]=="hf": return {**normalizar_turno(g[0],g[1]),"det":True,"pat":p["n"]}
            sh2,sm2=g[0],g[1] or "00"
            eh2=g[2] if len(g)>2 else None;em2=g[3] if len(g)>3 and g[3] else "00"
            if not eh2: continue
            result=normalizar_turno(f"{sh2}:{sm2}",f"{eh2}:{em2}")
            return {**result,"det":True,"pat":p["n"]}
    if (FK.search(t) or FK2.search(t)) and not FQ.search(t): return {"det":True,"pat":"kw","needs_llm":True}
    return {"det":False}

def normalizar_horas(texto):
    t=texto
    def tarde_fix(m):
        h=int(m.group(1)); return str(h+12 if h<12 else h)
    t=re.sub(r'(\d{1,2})\s*(?:de la tarde|de la noche|pm|p\.m\.)',tarde_fix,t,flags=re.I)
    t=re.sub(r'(\d{1,2})\s*(?:de la ma.ana|am|a\.m\.)',r'\1',t,flags=re.I)
    return t


def extraer_fecha(texto):
    """Extract date reference from message."""
    from datetime import date,timedelta
    t=texto.lower()
    hoy=date.today()
    if "ayer" in t: return (hoy-timedelta(days=1)).isoformat()
    if "anteayer" in t or "antes de ayer" in t: return (hoy-timedelta(days=2)).isoformat()
    import re as re2
    dm=re2.search(r'(\d{1,2})[/\-](\d{1,2})(?:[/\-](\d{2,4}))?',t)
    if dm:
        d,m=int(dm.group(1)),int(dm.group(2))
        y=int(dm.group(3)) if dm.group(3) else hoy.year
        if y<100: y+=2000
        if 1<=d<=31 and 1<=m<=12:
            try: return date(y,m,d).isoformat()
            except: pass
    return hoy.isoformat()

# ══════════════ MINI LLM NORMALIZADOR ══════════════
FICHAJE_LLM_SYS="""Eres un parser de fichajes laborales. Tu UNICO trabajo es extraer horas de entrada y salida.
REGLAS: SOLO JSON, NO inventas horas, NO conversas. Interpreta "de nueve a cinco"=09:00-17:00.
Rumano: "de la 9 la 17"=09:00-17:00. "y media"=:30, "y cuarto"=:15, "y pico"=:00.
Si no puedes determinar horas, devuelve es_fichaje false.
JSON: {"es_fichaje":true,"entrada":"09:00","salida":"17:00","overnight":false,"confianza":0.85}
o: {"es_fichaje":false,"confianza":0.2}"""

async def mini_llm_fichaje(texto,trace_id=""):
    """LLM fallback for ambiguous fichaje messages. Returns structured result."""
    t0=time.time()
    raw=await gpt(f'Extrae horas de fichaje: "{texto[:300]}"',FICHAJE_LLM_SYS,"gpt-4o-mini",200)
    ms=int((time.time()-t0)*1000)
    log.info(f"[{trace_id}] Mini LLM ({ms}ms): {raw[:100]}")
    try:
        if "```" in raw: raw=raw.split("```")[1].replace("json","").strip()
        d=json.loads(raw.strip())
    except:
        return {"es_fichaje":False,"confianza":0,"metodo":"llm","parseo_ms":ms}
    if not d.get("es_fichaje"):
        return {"es_fichaje":False,"confianza":d.get("confianza",0),"metodo":"llm","parseo_ms":ms}
    ent=d.get("entrada","");sal=d.get("salida","");conf=float(d.get("confianza",0.5))
    if conf<LLM_CONF_MEDIUM:
        return {"es_fichaje":False,"confianza":conf,"metodo":"llm","parseo_ms":ms,"rejected":True}
    if conf<LLM_CONF_HIGH:
        return {"es_fichaje":True,"confianza":conf,"metodo":"llm","parseo_ms":ms,"needs_clarification":True}
    turno=normalizar_turno(ent.replace(":",""),sal.replace(":","")) if ent and sal else {"ok":False}
    if not turno.get("ok"):
        turno=normalizar_turno(ent,sal)
    return {**turno,"es_fichaje":True,"confianza":conf,"metodo":"llm","parseo_ms":ms}

# ══════════════ IDEMPOTENCIA ══════════════
def generar_signature(emp_id,fecha,entrada,salida,obra_id=None):
    """Provisional sig (obra_id=None) for BIA-level dedup. Final sig (with obra_id) for fichajes_tramos."""
    raw=f"{emp_id}|{fecha}|{entrada}|{salida}|{obra_id or 'pending'}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

async def check_idempotencia(signature,provisional=False):
    """Check idempotencia. provisional=True checks recent logs (5min dedup). False checks fichajes_tramos (final)."""
    if provisional:
        rows=await db_get("bia_fichajes_log",f"signature=eq.{signature}&resultado=eq.registrado&order=created_at.desc&limit=1&select=created_at")
        if rows:
            from datetime import datetime,timezone,timedelta
            try:
                ct=rows[0].get("created_at","")
                if "T" in ct:
                    created=datetime.fromisoformat(ct.replace("Z","+00:00"))
                    if datetime.now(timezone.utc)-created<timedelta(minutes=5): return True
            except: pass
        return False
    rows=await db_get("fichajes_tramos",f"signature=eq.{signature}&select=id&limit=1")
    return len(rows)>0

# ══════════════ LOG FICHAJE DETALLADO ══════════════
async def log_fichaje(trace_id,emp_id,tel,msg_orig,msg_norm,patron,metodo,horas_raw,
                      entrada,salida,overnight,inferred_pm,ambiguous,dur,conf_llm,
                      signature,resultado,error_code,parseo_ms):
    await db_post("bia_fichajes_log",{
        "trace_id":trace_id,"empleado_id":emp_id,"telefono":tel,
        "mensaje_original":(msg_orig or "")[:500],"mensaje_normalizado":(msg_norm or "")[:500],
        "patron_regex":patron,"metodo":metodo,"horas_raw":horas_raw,
        "entrada":entrada,"salida":salida,"overnight":overnight,"inferred_pm":inferred_pm,
        "ambiguous":ambiguous,"duracion_horas":dur,"confianza_llm":conf_llm,
        "signature":signature,"resultado":resultado,"error_code":error_code,"parseo_ms":parseo_ms
    })


# ══════════════ SISTEMA DE INTENTS AUTOMÁTICOS ══════════════
from datetime import timedelta

INTENTS = [
    # ═══ SELF SCOPE — all roles ═══
    {"id":"horas_mes","kw":["cuántas horas","cuantas horas","horas este mes","horas llevo este mes","horas del mes"],"kw_ro":["câte ore","ore luna asta","ore luna aceasta"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"horas_hoy","kw":["cuántas horas hoy","cuantas horas hoy","horas de hoy","horas llevo hoy"],"kw_ro":["câte ore azi","ore azi"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"horas_semana","kw":["horas esta semana","horas semana","horas llevo esta semana"],"kw_ro":["ore săptămâna asta","câte ore săptămâna"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"horas_ayer","kw":["horas hice ayer","horas ayer","trabajé ayer","trabaje ayer"],"kw_ro":["câte ore ieri","ore ieri"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"fichajes_hoy","kw":["mis fichajes de hoy","fichajes tengo hoy","fichajes hoy"],"kw_ro":["pontaj azi","pontajele mele"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"he_fichado_hoy","kw":["he fichado hoy","ya he fichado","tengo fichaje hoy"],"kw_ro":["am pontat azi","am făcut pontaj"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"entrada_hoy","kw":["hora entré hoy","hora de entrada hoy","cuando entre hoy","a que hora entre"],"kw_ro":["la ce oră am intrat","ora de intrare"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"salida_ayer","kw":["hora salí ayer","hora de salida ayer","cuando sali ayer"],"kw_ro":["la ce oră am ieșit ieri","ora de ieșire ieri"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"obra_actual","kw":["en qué obra estoy","cuál es mi obra","qué obra tengo","mi obra"],"kw_ro":["la ce lucrare sunt","lucrarea mea","ce lucrare am"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"encargado_mi_obra","kw":["quién es el encargado","encargado de mi obra"],"kw_ro":["cine e șeful","responsabil lucrare"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"direccion_obra","kw":["dónde está la obra","dirección de la obra","donde trabajo"],"kw_ro":["unde e lucrarea","adresa lucrării"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"anticipos_mios","kw":["anticipos pendientes","mis anticipos","cuántos anticipos"],"kw_ro":["am avansuri","avansuri","câte avansuri"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"importe_anticipos","kw":["dinero he pedido de anticipo","importe de mis anticipos","dinero anticipado"],"kw_ro":["cât avans am cerut","suma avansuri"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"ultimo_anticipo","kw":["último anticipo","ultimo anticipo","cuando pedi el ultimo anticipo"],"kw_ro":["ultimul avans","când am cerut ultimul"],"tipo":"query","scope":"self","roles":[1,2,3]},
    {"id":"ganado_mes","kw":["cuánto he ganado","cuanto he ganado","cuánto cobraré","dinero de este mes"],"kw_ro":["cât am câștigat luna asta","cât iau luna asta"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"valor_horas","kw":["cuánto valen mis horas","valor de mis horas","dinero por horas"],"kw_ro":["cât valorează orele","valoare ore"],"tipo":"query_calc","scope":"self","roles":[1,2,3]},
    {"id":"como_ficho","kw":["cómo ficho","como ficho","como registrar horas","como se ficha"],"kw_ro":["cum pontez","cum înregistrez orele"],"tipo":"fixed","scope":"self","roles":[1,2,3]},
    {"id":"como_mando_gasto","kw":["cómo mando un gasto","como envío una factura","como mando un ticket","como enviar factura"],"kw_ro":["cum trimit o cheltuială","cum trimit o factură","cum trimit bonul"],"tipo":"fixed","scope":"self","roles":[1,2,3]},
    {"id":"como_pido_anticipo","kw":["cómo pedir un anticipo","como se pide un anticipo","como pido anticipo"],"kw_ro":["cum cer un avans","cum se cere avans"],"tipo":"fixed","scope":"self","roles":[1,2,3]},
    # ═══ TEAM SCOPE — admin + encargado ═══
    {"id":"quien_ficho_hoy","kw":["quién ha fichado hoy","quien ficho hoy","fichajes del equipo"],"kw_ro":["cine a pontat azi","pontaje echipa"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"quien_no_ficho","kw":["quién no ha fichado","quien falta por fichar","empleados que faltan"],"kw_ro":["cine nu a pontat","cine lipsește"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"horas_equipo_hoy","kw":["horas lleva el equipo hoy","horas del equipo hoy","horas equipo hoy"],"kw_ro":["ore echipa azi","câte ore are echipa azi"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"horas_equipo_semana","kw":["horas equipo semana","horas lleva el equipo esta semana"],"kw_ro":["ore echipa săptămâna","câte ore are echipa săptămâna"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"empleados_obra","kw":["quién trabaja en","empleados de la obra","qué empleados hay"],"kw_ro":["cine lucrează la","angajați lucrare"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"gastos_obra_total","kw":["cuánto se ha gastado en","gastos de esta obra","total gastos obra"],"kw_ro":["cât s-a cheltuit pe lucrare","cheltuieli lucrare"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"gastos_obra_semana","kw":["gastos obra semana","gastado esta semana en"],"kw_ro":["cheltuit săptămâna asta pe lucrare"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"ultimo_gasto_obra","kw":["último gasto de","ultimo gasto obra"],"kw_ro":["ultima cheltuială","ultimul cost lucrare"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"facturas_obra","kw":["facturas hay en","facturas de la obra"],"kw_ro":["facturi la lucrare","ce facturi sunt"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"anticipos_pendientes","kw":["anticipos hay pendientes","anticipos pendientes empresa"],"kw_ro":["avansuri în așteptare","avansuri pendinte"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"dinero_anticipado","kw":["cuánto dinero se ha adelantado","total anticipos empresa"],"kw_ro":["cât s-a dat avans total","total avansuri"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    # ═══ TEAM SCOPE — NEW INTENTS ═══
    {"id":"coste_obra","kw":["cuánto gastado en obra","coste obra","gasto total obra","cuanto nos hemos gastado","gastado en la obra","gasto en la obra","que gasto tenemos","gastos de la obra","gasto obra","cuanto llevamos gastado"],"kw_ro":["cât s-a cheltuit pe lucrare","cost lucrare"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"gastos_empleado","kw":["cuanto dinero ha gastado","cuanto gasto","facturas de","gastos de","cuanto a gastado","dinero gastado"],"kw_ro":["cat a cheltuit","cheltuieli de"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"horas_obra","kw":["horas se ha trabajado en","horas en la obra","horas obra","cuantas horas en obra","horas trabajadas en"],"kw_ro":["ore lucrate pe lucrare","cate ore pe lucrare"],"tipo":"query_calc","scope":"team","roles":[1,2]},
    {"id":"empleados_en_obra","kw":["empleados trabajaron en","quien trabajo en","trabajaron en la obra","empleados en obra"],"kw_ro":["cine a lucrat la","angajati pe lucrare"],"tipo":"query","scope":"team","roles":[1,2]},
    {"id":"lista_empleados","kw":["empleados estan trabajando","lista empleados","que empleados hay","empleados de la empresa"],"kw_ro":["angajati firma","lista angajati","cati angajati"],"tipo":"query","scope":"team","roles":[1,2]},
    # ═══ ADMIN SCOPE ═══
    {"id":"obras_activas","kw":["obras están activas","cuántas obras","obras abiertas"],"kw_ro":["lucrări active","câte lucrări sunt active"],"tipo":"query","scope":"admin","roles":[1,2]},
    {"id":"obra_mas_gasto","kw":["obra tiene más gasto","obra con más gasto"],"kw_ro":["care lucrare are cele mai multe cheltuieli"],"tipo":"query_calc","scope":"admin","roles":[1,2]},
    {"id":"empleados_totales","kw":["cuántos empleados hay","empleados activos"],"kw_ro":["câți angajați sunt","angajați activi"],"tipo":"query","scope":"admin","roles":[1]},
    {"id":"gastos_empresa_hoy","kw":["cuánto se ha gastado hoy","gastos de hoy empresa"],"kw_ro":["cât s-a cheltuit azi","cheltuieli azi"],"tipo":"query_calc","scope":"admin","roles":[1]},
    {"id":"gastos_empresa_mes","kw":["cuánto se ha gastado este mes","gastos del mes","total gastos mes"],"kw_ro":["cât s-a cheltuit luna asta","cheltuieli luna"],"tipo":"query_calc","scope":"admin","roles":[1]},
    {"id":"horas_empresa_mes","kw":["horas lleva la empresa","horas empresa mes"],"kw_ro":["ore firma luna asta","câte ore are firma"],"tipo":"query_calc","scope":"admin","roles":[1]},
    {"id":"pagos_pendientes","kw":["pagos pendientes","empleados tienen pagos pendientes"],"kw_ro":["plăți restante","ce angajați au plăți"],"tipo":"query","scope":"admin","roles":[1]},
    {"id":"resumen_hoy","kw":["resumen de hoy","dame un resumen de hoy"],"kw_ro":["rezumat de azi","rezumat azi"],"tipo":"query_calc","scope":"admin","roles":[1,2]},
    {"id":"resumen_gastos_mes","kw":["resumen de gastos del mes","resumen gastos mes"],"kw_ro":["rezumat cheltuieli luna"],"tipo":"query_calc","scope":"admin","roles":[1,2]},
]

def detectar_intent(texto, rol):
    """Match text against intent keywords. Returns intent dict or None."""
    t = texto.lower().strip()
    # Remove accents for matching
    import unicodedata
    def norm(s): return ''.join(c for c in unicodedata.normalize('NFD',s) if unicodedata.category(c)!='Mn')
    tn = norm(t)
    best = None
    best_score = 0
    for intent in INTENTS:
        if rol not in intent["roles"]: continue
        for kw in intent["kw"] + intent.get("kw_ro", []):
            kwn = norm(kw.lower())
            exact_match = tn == kwn
            bounded_match = len(kwn) >= 10 and re.search(rf'(?<!\w){re.escape(kwn)}(?!\w)', tn)
            score = len(kwn) + (1000 if exact_match else 0)
            if (exact_match or bounded_match) and score > best_score:
                best = intent
                best_score = score
    return best

# ═══ DATE HELPERS ═══
def _hoy(): return datetime.now().strftime("%Y-%m-%d")
def _ayer(): return (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
def _inicio_semana():
    d=datetime.now(); return (d-timedelta(days=d.weekday())).strftime("%Y-%m-%d")
def _fin_semana():
    d=datetime.now(); return (d+timedelta(days=6-d.weekday())).strftime("%Y-%m-%d")
def _inicio_mes(): return datetime.now().strftime("%Y-%m-01")
def _fin_mes():
    d=datetime.now(); m=d.month; y=d.year
    if m==12: return f"{y+1}-01-01"
    return f"{y}-{m+1:02d}-01"

# ═══ INTENT HANDLERS ═══
FIXED_RESPONSES = {
    "como_ficho": "Puedes fichar mandando algo como *9-17* o *8:30-16* por WhatsApp \U0001f44d También vale *de 9 a 17*, *entrado 9 salido 17*, o un audio.",
    "como_mando_gasto": "Manda una foto o PDF del ticket/factura por WhatsApp. BIA lo lee automáticamente y te pregunta la obra \U0001f60a",
    "como_pido_anticipo": "Dile a tu encargado o al admin el importe que necesitas. Ellos lo registran en el sistema.",
}

async def ejecutar_intent(s, intent):
    """Execute an intent handler and return response string."""
    iid = intent["id"]
    eid = s.empleado.get("id", 0)
    rol = int(s.empleado.get("rol_id", 99) or 99)
    nombre = s.empleado.get("apodo") or s.empleado.get("nombre", "")
    
    # FIXED responses
    if iid in FIXED_RESPONSES:
        return FIXED_RESPONSES[iid]
    
    # ═══ SELF: Horas por período ═══
    if iid == "horas_hoy":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_hoy()}&select=horas_decimal,obra_nombre")
        if not rows: return f"Hoy no tienes fichajes registrados, {nombre} \U0001f914"
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        obras = set(r.get("obra_nombre", "?") for r in rows)
        return f"Hoy llevas *{total}h* trabajadas en {', '.join(obras)} \U0001f44d"
    
    if iid == "horas_ayer":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_ayer()}&select=horas_decimal,hora_inicio,hora_fin")
        if not rows: return f"Ayer no tienes fichajes, {nombre}"
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        return f"Ayer hiciste *{total}h* \U0001f4aa"
    
    if iid == "horas_semana":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=gte.{_inicio_semana()}&fecha=lte.{_fin_semana()}&select=horas_decimal")
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        return f"Esta semana llevas *{total}h* trabajadas \U0001f4aa"
    
    if iid == "horas_mes":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=gte.{_inicio_mes()}&fecha=lt.{_fin_mes()}&select=horas_decimal,obra_nombre")
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        obras = set(r.get("obra_nombre", "") for r in rows if r.get("obra_nombre"))
        return f"Llevas *{total}h* este mes en {len(obras)} obras \U0001f4ca"
    
    # ═══ SELF: Fichajes ═══
    if iid == "fichajes_hoy":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_hoy()}&select=hora_inicio,hora_fin,horas_decimal,obra_nombre&order=hora_inicio")
        if not rows: return f"Hoy no tienes fichajes, {nombre}"
        det = "\n".join([f"  \U0001f552 {str(r.get('hora_inicio',''))[:5]}-{str(r.get('hora_fin',''))[:5]} ({r.get('horas_decimal',0)}h) en *{r.get('obra_nombre','?')}*" for r in rows])
        return f"Tus fichajes de hoy:\n{det}"
    
    if iid == "he_fichado_hoy":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_hoy()}&select=id&limit=1")
        if rows: return f"Sí, hoy ya has fichado \u2705"
        return f"No, hoy todavía no has fichado \u274c"
    
    if iid == "entrada_hoy":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_hoy()}&select=hora_inicio&order=hora_inicio.asc&limit=1")
        if not rows: return "Hoy no tienes fichaje registrado"
        return f"Hoy entraste a las *{str(rows[0].get('hora_inicio','?'))[:5]}*"
    
    if iid == "salida_ayer":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=eq.{_ayer()}&select=hora_fin&order=hora_fin.desc&limit=1")
        if not rows: return "Ayer no tienes fichaje registrado"
        return f"Ayer saliste a las *{str(rows[0].get('hora_fin','?'))[:5]}*"
    
    # ═══ SELF: Obra info ═══
    if iid == "obra_actual":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&order=fecha.desc,hora_inicio.desc&select=obra_nombre,obra_id&limit=1")
        if not rows: return "No tienes fichajes recientes para determinar tu obra"
        return f"Tu última obra es *{rows[0].get('obra_nombre','?')}* \U0001f3d7\ufe0f"
    
    if iid == "encargado_mi_obra":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&order=fecha.desc&select=obra_id&limit=1")
        if not rows: return "No pude determinar tu obra actual"
        obra = await db_get("obras", f"id=eq.{rows[0].get('obra_id','0')}&select=nombre,encargado_id")
        if not obra or not obra[0].get("encargado_id"): return "No hay encargado asignado a tu obra"
        enc = await db_get("empleados", f"id=eq.{obra[0]['encargado_id']}&select=nombre")
        return f"El encargado de *{obra[0].get('nombre','?')}* es *{enc[0].get('nombre','?') if enc else '?'}*"
    
    if iid == "direccion_obra":
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&order=fecha.desc&select=obra_id&limit=1")
        if not rows: return "No pude determinar tu obra"
        obra = await db_get("obras", f"id=eq.{rows[0].get('obra_id','0')}&select=nombre,direccion")
        if not obra: return "No encontré la obra"
        return f"*{obra[0].get('nombre','?')}* está en *{obra[0].get('direccion','dirección no registrada')}*"
    
    # ═══ SELF: Anticipos ═══
    if iid == "anticipos_mios":
        rows = await db_get("anticipos", f"empleado_id=eq.{eid}&select=id,importe,fecha,estado&order=created_at.desc&limit=10")
        if not rows: return f"No tienes anticipos registrados, {nombre}"
        return f"Tienes *{len(rows)}* anticipos. Último: {rows[0].get('importe',0)}\u20ac el {rows[0].get('fecha','?')}"
    
    if iid == "importe_anticipos":
        rows = await db_get("anticipos", f"empleado_id=eq.{eid}&select=importe")
        total = round(sum(float(r.get("importe", 0) or 0) for r in rows), 2)
        return f"Llevas *{total}\u20ac* en anticipos"
    
    if iid == "ultimo_anticipo":
        rows = await db_get("anticipos", f"empleado_id=eq.{eid}&order=created_at.desc&limit=1&select=importe,fecha")
        if not rows: return "No tienes anticipos"
        return f"Tu último anticipo fue de *{rows[0].get('importe',0)}\u20ac* el {rows[0].get('fecha','?')}"
    
    # ═══ SELF: Dinero ═══
    if iid in ("ganado_mes", "valor_horas"):
        rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid}&fecha=gte.{_inicio_mes()}&fecha=lt.{_fin_mes()}&select=horas_decimal,coste_hora,coste_total")
        total_h = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        total_e = round(sum(float(r.get("coste_total", 0) or 0) for r in rows), 2)
        return f"Llevas *{total_h}h* este mes \u2192 aproximadamente *{total_e}\u20ac* \U0001f4b6"
    
    # ═══ TEAM: Fichajes equipo ═══
    if iid == "quien_ficho_hoy":
        rows = await db_get("fichajes_tramos", f"fecha=eq.{_hoy()}&select=empleado_nombre&order=empleado_nombre")
        nombres = sorted(set(r.get("empleado_nombre", "?") for r in rows))
        if not nombres: return "Hoy no ha fichado nadie todavía"
        return f"Hoy han fichado *{len(nombres)}* personas:\n" + "\n".join([f"  \u2705 {n}" for n in nombres])
    
    if iid == "quien_no_ficho":
        activos = await db_get("empleados", "estado=eq.Activo&select=id,nombre")
        fichados = await db_get("fichajes_tramos", f"fecha=eq.{_hoy()}&select=empleado_id")
        fichados_ids = set(r.get("empleado_id") for r in fichados)
        faltan = [e for e in activos if e["id"] not in fichados_ids]
        if not faltan: return "Todos han fichado hoy \u2705"
        return f"Faltan por fichar *{len(faltan)}*:\n" + "\n".join([f"  \u274c {e['nombre']}" for e in faltan])
    
    if iid == "horas_equipo_hoy":
        rows = await db_get("fichajes_tramos", f"fecha=eq.{_hoy()}&select=horas_decimal")
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        return f"El equipo lleva *{total}h* hoy \U0001f477"
    
    if iid == "horas_equipo_semana":
        rows = await db_get("fichajes_tramos", f"fecha=gte.{_inicio_semana()}&fecha=lte.{_fin_semana()}&select=horas_decimal")
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        return f"Esta semana el equipo lleva *{total}h*"
    
    if iid == "empleados_obra":
        obras = await db_get("obras", "estado=eq.En curso&select=id,nombre")
        if not obras: return "No hay obras activas"
        # Use last obra mentioned or first
        lista = "\n".join([f"  \U0001f3d7\ufe0f *{o['nombre']}*" for o in obras[:10]])
        return f"Obras activas:\n{lista}\n\nDime cuál y te digo los empleados"
    
    # ═══ TEAM: Gastos obra ═══
    if iid == "gastos_obra_total":
        rows = await db_get("gastos", "select=total,obra&order=created_at.desc&limit=100")
        total = round(sum(float(r.get("total", 0) or 0) for r in rows), 2)
        return f"Total gastos registrados: *{total}\u20ac* en {len(rows)} facturas \U0001f4b8"
    
    if iid == "ultimo_gasto_obra":
        rows = await db_get("gastos", "order=created_at.desc&limit=1&select=concepto,total,proveedor,obra,fecha_factura")
        if not rows: return "No hay gastos registrados"
        g = rows[0]
        return f"Último gasto: *{g.get('proveedor','?')}* — {g.get('total',0)}\u20ac ({g.get('obra','?')}) el {g.get('fecha_factura','?')}"
    
    if iid == "facturas_obra":
        rows = await db_get("gastos", "order=created_at.desc&limit=10&select=proveedor,total,obra,fecha_factura")
        if not rows: return "No hay facturas registradas"
        det = "\n".join([f"  \U0001f9fe {r.get('proveedor','?')} — {r.get('total',0)}\u20ac ({r.get('obra','?')})" for r in rows[:10]])
        return f"Últimas facturas:\n{det}"
    
    if iid == "anticipos_pendientes":
        rows = await db_get("anticipos", "estado=in.(pendiente,solicitado,PENDIENTE,SOLICITADO)&select=empleado_id,importe&order=created_at.desc")
        if not rows: return "No hay anticipos pendientes \u2705"
        total = round(sum(float(r.get("importe", 0) or 0) for r in rows), 2)
        return f"Hay *{len(rows)}* anticipos pendientes por *{total}\u20ac*"
    
    if iid == "dinero_anticipado":
        rows = await db_get("anticipos", "select=importe")
        total = round(sum(float(r.get("importe", 0) or 0) for r in rows), 2)
        return f"Total anticipado: *{total}\u20ac*"
    
    # ═══ ADMIN: Empresa ═══
    if iid == "obras_activas":
        rows = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
        if not rows: return "No hay obras activas"
        lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(rows)])
        return f"*{len(rows)}* obras activas:\n{lista}"
    
    if iid == "empleados_totales":
        rows = await db_get("empleados", "estado=eq.Activo&select=id,nombre&order=nombre")
        return f"Hay *{len(rows)}* empleados activos"
    
    if iid == "gastos_empresa_hoy":
        rows = await db_get("gastos", f"fecha_factura=eq.{_hoy()}&select=total")
        total = round(sum(float(r.get("total", 0) or 0) for r in rows), 2)
        return f"Hoy lleváis *{total}\u20ac* de gasto" if rows else "Hoy no hay gastos registrados"
    
    if iid == "gastos_empresa_mes":
        rows = await db_get("gastos", f"fecha_factura=gte.{_inicio_mes()}&fecha_factura=lt.{_fin_mes()}&select=total")
        total = round(sum(float(r.get("total", 0) or 0) for r in rows), 2)
        return f"Este mes lleváis *{total}\u20ac* de gasto total \U0001f4ca"
    
    if iid == "horas_empresa_mes":
        rows = await db_get("fichajes_tramos", f"fecha=gte.{_inicio_mes()}&fecha=lt.{_fin_mes()}&select=horas_decimal")
        total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
        return f"La empresa lleva *{total}h* este mes"
    
    if iid == "obra_mas_gasto":
        rows = await db_get("gastos", "select=obra,total&order=created_at.desc&limit=500")
        if not rows: return "No hay gastos"
        from collections import Counter
        por_obra = {}
        for r in rows:
            o = r.get("obra", "?")
            por_obra[o] = por_obra.get(o, 0) + float(r.get("total", 0) or 0)
        top = sorted(por_obra.items(), key=lambda x: -x[1])[:3]
        det = "\n".join([f"  {i+1}. *{o}*: {round(v,2)}\u20ac" for i, (o, v) in enumerate(top)])
        return f"Obras con más gasto:\n{det}"
    
    if iid == "pagos_pendientes":
        rows = await db_get("pagos_nomina", "estado=in.(pendiente,PENDIENTE)&select=empleado_id,importe")
        if not rows: return "No hay pagos pendientes \u2705"
        return f"Hay *{len(rows)}* pagos pendientes"
    
    if iid == "resumen_hoy":
        fich = await db_get("fichajes_tramos", f"fecha=eq.{_hoy()}&select=horas_decimal,empleado_nombre")
        gast = await db_get("gastos", f"fecha_factura=eq.{_hoy()}&select=total")
        h = round(sum(float(r.get("horas_decimal", 0) or 0) for r in fich), 1)
        g = round(sum(float(r.get("total", 0) or 0) for r in gast), 2)
        n = len(set(r.get("empleado_nombre","") for r in fich))
        return f"\U0001f4ca *Resumen de hoy:*\n  \U0001f552 {h}h trabajadas\n  \U0001f477 {n} empleados ficharon\n  \U0001f4b8 {g}\u20ac en gastos"
    
    if iid == "resumen_gastos_mes":
        rows = await db_get("gastos", f"fecha_factura=gte.{_inicio_mes()}&fecha_factura=lt.{_fin_mes()}&select=total,obra")
        total = round(sum(float(r.get("total", 0) or 0) for r in rows), 2)
        por_obra = {}
        for r in rows:
            o = r.get("obra", "Sin obra")
            por_obra[o] = por_obra.get(o, 0) + float(r.get("total", 0) or 0)
        top = sorted(por_obra.items(), key=lambda x: -x[1])[:5]
        det = "\n".join([f"  \U0001f3d7\ufe0f *{o}*: {round(v,2)}\u20ac" for o, v in top])
        return f"\U0001f4ca *Gastos del mes: {total}\u20ac*\n\nPor obra:\n{det}"
    
    # ═══ COSTE TOTAL OBRA ═══
    if iid == "coste_obra":
        obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
        if not obras: return "No hay obras activas"
        lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
        await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid, "tipo": "coste_obra_sel", "dominio": "INTENT", "contexto": {"obras_ids": [o["id"] for o in obras], "obras_nombres": [o["nombre"] for o in obras]}})
        return f"\U0001f4b0 Que obra quieres consultar?\n\n{lista}\n\nDime numero o nombre"
    
    # ═══ GASTOS DE EMPLEADO ═══
    if iid == "gastos_empleado":
        texto_lower = s.mensaje_normalizado.lower()
        emps = await db_get("empleados", "estado=eq.Activo&select=id,nombre&order=nombre")
        target_emp = None
        for emp_item in emps:
            for part in emp_item.get("nombre", "").lower().split():
                if len(part) > 2 and part in texto_lower:
                    target_emp = emp_item; break
            if target_emp: break
        if not target_emp: return "No identifique al empleado. Dime el nombre exacto \U0001f914"
        fecha = extraer_fecha(texto_lower)
        gastos_rows = await db_get("gastos", f"empleado_id=eq.{target_emp['id']}&created_at=gte.{fecha}T00:00:00&created_at=lt.{fecha}T23:59:59&select=proveedor,total,obra,concepto&order=created_at.desc")
        if not gastos_rows:
            nombre_search = target_emp["nombre"].split()[0]
            gastos_rows = await db_get("gastos", f"empleado_nombre=ilike.*{nombre_search}*&created_at=gte.{fecha}T00:00:00&created_at=lt.{fecha}T23:59:59&select=proveedor,total,obra,concepto&order=created_at.desc")
        if not gastos_rows:
            fecha_txt = "hoy" if fecha == _hoy() else ("ayer" if fecha == _ayer() else fecha)
            return f"*{target_emp['nombre']}* no tiene facturas {fecha_txt}"
        total = round(sum(float(r.get("total", 0) or 0) for r in gastos_rows), 2)
        fecha_txt = "hoy" if fecha == _hoy() else ("ayer" if fecha == _ayer() else fecha)
        det = "\n".join([f"  \U0001f9fe *{r.get('proveedor','?')}* \u2014 {r.get('total',0)}\u20ac\n     Obra: {r.get('obra','?')}" for r in gastos_rows])
        return f"\U0001f4b8 *Gastos de {target_emp['nombre']} {fecha_txt}:*\n\n{det}\n\n\u2500\u2500\u2500\n\U0001f4b0 *TOTAL: {total}\u20ac* ({len(gastos_rows)} facturas)"
    
    # ═══ HORAS EN OBRA ═══
    if iid == "horas_obra":
        obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
        if not obras: return "No hay obras activas"
        lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
        texto_lower = s.mensaje_normalizado.lower()
        periodo = "todo"
        if "hoy" in texto_lower or "azi" in texto_lower: periodo = "hoy"
        elif "ayer" in texto_lower or "ieri" in texto_lower: periodo = "ayer"
        elif "semana" in texto_lower or "saptamana" in texto_lower: periodo = "semana"
        elif "mes" in texto_lower or "luna" in texto_lower: periodo = "mes"
        await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid, "tipo": "horas_obra_sel", "dominio": "INTENT", "contexto": {"obras_ids": [o["id"] for o in obras], "obras_nombres": [o["nombre"] for o in obras], "periodo": periodo}})
        return f"\U0001f552 Que obra? Periodo: *{periodo}*\n\n{lista}\n\nDime numero o nombre"
    
    # ═══ EMPLEADOS EN OBRA ═══
    if iid == "empleados_en_obra":
        obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
        if not obras: return "No hay obras activas"
        lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
        texto_lower = s.mensaje_normalizado.lower()
        periodo = "todo"
        if "hoy" in texto_lower: periodo = "hoy"
        elif "ayer" in texto_lower: periodo = "ayer"
        elif "semana" in texto_lower: periodo = "semana"
        elif "mes" in texto_lower: periodo = "mes"
        await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid, "tipo": "empleados_obra_sel", "dominio": "INTENT", "contexto": {"obras_ids": [o["id"] for o in obras], "obras_nombres": [o["nombre"] for o in obras], "periodo": periodo}})
        return f"\U0001f477 Que obra? Periodo: *{periodo}*\n\n{lista}\n\nDime numero o nombre"
    
    # ═══ LISTA EMPLEADOS ═══
    if iid == "lista_empleados":
        emps = await db_get("empleados", "estado=eq.Activo&select=id,nombre,cargo&order=nombre")
        if not emps: return "No hay empleados activos"
        det = "\n".join([f"  \U0001f477 *{e.get('nombre','?')}* \u2014 {e.get('cargo','?')}" for e in emps])
        return f"*{len(emps)} empleados activos:*\n\n{det}"
    
    return None  # Intent not handled

# ══════════════ DETECTOR ══════════════
PS=re.compile(r'^(hola|buenos d[ií]as|buenas( tardes| noches)?|qu[eé] tal|hey)[\s!.?]*$',re.I)
PC=re.compile(r'\b(confirmo|todos ok|confirmado|ok equipo|vale[,.]?\s*confirmado)\b',re.I)

def detectar(txt,empleado_rol=0,tiene_espera_conf=False,txt_original=""):
    t=txt.lower().strip()
    pf=parse_fichaje(t)
    if pf.get("det"): return "FICHAJE","procesar",1.0
    # Protected confirmation
    if PC.search(t):
        if empleado_rol in(1,2) or tiene_espera_conf:
            return "FICHAJE","confirmar",0.9
        # Not valid confirmation context — fall through to general
    if PS.match(t): return "SALUDO","responder",1.0
    # ═══ COMANDOS EXACTOS EN MAYÚSCULAS (si una letra falla, no ejecuta) ═══
    cmd = txt_original.strip()

    if cmd == "REABRIR OBRA": return "REABRIR_OBRA","reabrir",1.0
    if cmd == "CERRAR OBRA": return "CERRAR_OBRA","cerrar",1.0
    if cmd == "ALTA OBRA": return "OBRA_ALTA","crear",1.0
    if cmd == "ALTA EMPLEADO": return "ALTA_EMPLEADO","crear",1.0
    if cmd == "BAJA EMPLEADO": return "BAJA_EMPLEADO","baja",1.0
    if cmd == "HORAS OBRA": return "CMD_HORAS_OBRA","paso1",1.0
    if cmd == "GASTOS EMPLEADO": return "CMD_GASTOS_EMP","paso1",1.0
    if cmd == "GASTOS OBRA": return "CMD_GASTOS_OBRA","paso1",1.0
    if cmd == "CALCULAR NOMINA": return "CMD_CALC_NOMINA","paso1",1.0
    if cmd == "ENVIAR NOMINA": return "CMD_ENVIAR_NOMINA","paso1",1.0
    if cmd == "ANTICIPO": return "CMD_ANTICIPO","paso1",1.0
    if cmd == "REGISTRAR ANTICIPO": return "CMD_REG_ANTICIPO","paso1",1.0
    # Normal obra detection (lowercase)
    if re.search(r'nueva obra|registra(r|me)?\s*obra|abrir obra|alta obra|dar de alta obra',t): return "OBRA_ALTA","crear",0.95
    if re.search(r'cerrar obra|baja obra|dar de baja',t): return "OBRA_BAJA","cerrar",0.9
    if re.search(r'(que me puedes decir de|que sabes de|informacion de|información de|quien es|quién es)\s+[a-záéíóúñ]+',t): return "EMPLEADOS","consultar",0.88
    if re.search(r'^de\s+[a-záéíóúñ]+(?:\s+[a-záéíóúñ]+)?\s+que me puedes decir',t): return "EMPLEADOS","consultar",0.88
    if re.search(r'n[oó]mina|sueldo|salario|cu[aá]nto (le )?debo|pagar a|calcul[ae]|env[ií]a(me)?\\s.*(n[oó]mina|documento)',t): return "NOMINA","calcular",0.95
    return "AMBIGUO","clasificar",0.0

# ══════════════ CLASIFICADOR GPT ══════════════
async def clasificar(txt):
    p=f'Clasifica en UN dominio y da confianza. Dominios: FICHAJE,OBRAS,FINANZAS,EMPLEADOS,NOMINA,DOCUMENTOS,INVENTARIO,GENERAL. JSON: {{"dominio":"OBRAS","confianza":0.85}}. Mensaje: "{txt[:500]}"'
    raw=await gpt(p)
    try:
        if "```" in raw: raw=raw.split("```")[1].replace("json","").strip()
        d=json.loads(raw.strip());dom=d.get("dominio","GENERAL").upper();conf=float(d.get("confianza",0.5))
        return (dom if dom in ["FICHAJE","OBRAS","FINANZAS","EMPLEADOS","NOMINA","DOCUMENTOS","INVENTARIO","GENERAL"] else "GENERAL"),conf
    except: return "GENERAL",0.3

def debe_consumir_espera(esp, texto, rol):
    """Only consume an espera when the message looks like a valid reply for that step."""
    t=(texto or "").strip()
    if not t:
        return False
    t_low=t.lower()
    tipo=esp.get("tipo","")

    if tipo=="n8n_pending":
        return True
    if tipo=="nomina_dni":
        return bool(re.match(r'^[A-Z0-9][A-Z0-9\- ]{6,14}$',t.upper()))
    if tipo=="confirmar_fichaje":
        return t_low in("si","sí","ok","vale","correcto","yes") or parse_fichaje(t_low).get("det",False)
    if tipo=="obra_madrid":
        return bool(re.search(r'\b(dentro|fuera|madrid|si|sí|no)\b',t_low))
    if tipo in("factura_obra","obra_baja","obra_encargado"):
        if t.isdigit():
            return True
        if "?" in t:
            return False
        if detectar_intent(t,rol):
            return False
        dom,_,_=detectar(t,rol,False)
        return dom=="AMBIGUO" and len(t.split())<=6
    if tipo in("obra_nombre","obra_direccion"):
        if "?" in t:
            return False
        if detectar_intent(t,rol):
            return False
        dom,_,_=detectar(t,rol,False)
        return dom=="AMBIGUO"
    if tipo in (
        "menu_admin","menu_enc","menu_emp","menu_fichar_obra","menu_encargado",
        "menu_calc_nomina","menu_cuanto_cobro","menu_horas_trab","menu_enviar_nomina","menu_anticipos",
        "coste_obra_sel","horas_obra_sel","empleados_obra_sel","reabrir_obra_sel","baja_empleado_sel"
    ):
        return t.isdigit()
    if tipo=="menu_fichar_dia":
        return t_low in ("1","2","hoy","ayer","azi","ieri")
    if tipo=="menu_fichar_horas":
        pf=parse_fichaje(normalizar_horas(t))
        return bool(pf.get("ok") and pf.get("entrada") and pf.get("salida"))
    return True

# ══════════════ AGENTE FICHAJE BLINDADO ══════════════
async def ag_fichaje(s):
    """Complete armored fichaje flow: regex → LLM → normalize → validate → idempotency → register → log"""
    s.timer_start("fichaje")
    t0=time.time()
    texto=s.mensaje_normalizado
    emp_id=s.empleado.get("id",0)
    hoy=extraer_fecha(texto)
    
    # Continuation from espera (obra selection) — rebuild backend state + send selection
    if s.accion=="continuar" and s.dominio_fuente=="espera":
        try:
            base_body={"empleado_id":emp_id,"empleado_nombre":s.empleado.get("nombre",""),
                       "empleado_telefono":s.empleado.get("telefono",""),"coste_hora":s.empleado.get("coste_hora",0),"fuera_madrid_hora":15}
            # Get original message from espera context to rebuild backend state
            esp_ctx=s.metadata.get("espera_contexto",{})
            msg_orig=esp_ctx.get("mensaje_original","")
            async with httpx.AsyncClient(timeout=30) as c:
                if msg_orig:
                    # Step 1: Resend original fichaje to rebuild backend state
                    log.info(f"[{s.trace_id}] Rebuilding backend state: {msg_orig[:50]}")
                    await c.post(f"{PYTHON_URL}/procesar-fichaje",json={**base_body,"mensaje":msg_orig})
                # Step 2: Send the obra selection
                r=await c.post(f"{PYTHON_URL}/procesar-fichaje",json={**base_body,"mensaje":texto})
                d=safe_json_response(r,f"[{s.trace_id}] fichaje_continuar",{})
                if d.get("_parse_error"):
                    raise ValueError(f"backend fichajes devolvio respuesta no JSON ({d.get('_http_status')})")
            msg=d.get("mensaje",d.get("message",str(d)))
            if d.get("error") or "error" in str(d).lower()[:50]:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
            orig_msg=esp_ctx.get("mensaje_original",texto)
            if "obra" in msg.lower() and "1." in msg:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"ok":True,"mensaje_original":orig_msg}})
            elif "?" in msg:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"continuation":True,"mensaje_original":orig_msg}})
            s.timer_end("fichaje"); return msg
        except Exception as e:
            s.add_error(f"Fichaje espera: {e}"); s.timer_end("fichaje")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
            return "Problema registrando el fichaje \U0001f527 Repite tus horas (ej: 9-17)"
    
    # Confirmation flow — pass through to backend
    if s.accion=="confirmar":
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                r=await c.post(f"{PYTHON_URL}/confirmar-fichajes",json={"respuesta":texto,"fecha":""})
                d=safe_json_response(r,f"[{s.trace_id}] fichaje_confirmar",{})
                if d.get("_parse_error"):
                    raise ValueError(f"backend confirmacion devolvio respuesta no JSON ({d.get('_http_status')})")
                msg=d.get("mensaje",d.get("message",str(d)))
            s.timer_end("fichaje"); return msg
        except Exception as e:
            s.add_error(f"Confirmar: {e}"); s.timer_end("fichaje"); return "Problema con la confirmacion \U0001f527"
    
    # ═══ STEP 1: Parse regex ═══
    pf=parse_fichaje(texto)
    patron=pf.get("pat","none")
    metodo="regex"
    entrada,salida,overnight,inferred_pm,ambiguous,dur,conf_llm=None,None,False,False,False,None,None
    horas_raw=json.dumps({"texto":texto[:100],"patron":patron})
    
    if pf.get("det") and pf.get("ok"):
        # Regex extracted valid hours
        entrada=pf["entrada"];salida=pf["salida"];overnight=pf.get("overnight",False)
        inferred_pm=pf.get("inferred_pm",False);dur=pf.get("dur",0)
        log.info(f"[{s.trace_id}] Regex OK ({patron}): {entrada}-{salida} {dur}h {'OV' if overnight else ''} {'PM' if inferred_pm else ''}")
    
    elif pf.get("det") and pf.get("ambiguous"):
        # Regex detected but AMBIGUOUS — ask for clarification
        ent_raw=pf.get("entrada","?");sal_raw=pf.get("salida","?")
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                         horas_raw,ent_raw,sal_raw,False,False,True,None,None,"","aclaracion","AMBIGUOUS_SHIFT",int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        return f"No estoy segura de las horas: {ent_raw} a {sal_raw}. Puedes repetir con formato claro? Ej: 9-17 \U0001f550"
    
    elif pf.get("det") and pf.get("needs_times") and not pf.get("needs_llm"):
        # "8horas" pattern — we know duration but need actual times
        solo_h=pf.get("solo_h",0)
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,"regex",
                         horas_raw,None,None,False,False,False,None,None,"","aclaracion","NEEDS_TIMES",int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        return f"{solo_h} horas, vale! Pero necesito entrada y salida. Ej: 9-17 \U0001f550"
    
    elif pf.get("det") and pf.get("needs_llm") and pf.get("anti_pattern"):
        # Anti-pattern detected (multi-person, multi-day, etc.) — ask for clarification, don't LLM
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,"anti","regex",
                         horas_raw,None,None,False,False,True,None,None,"","aclaracion","ANTI_PATTERN",int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        return "No estoy segura de ese mensaje. Dime solo TUS horas de hoy, ej: 9-17 \U0001f550"
    
    elif pf.get("det") and pf.get("needs_llm"):
        # ═══ STEP 1B: Mini LLM fallback ═══
        metodo="llm"
        log.info(f"[{s.trace_id}] Fallback LLM para: {texto[:60]}")
        llm_r=await mini_llm_fichaje(texto,s.trace_id)
        conf_llm=llm_r.get("confianza",0)
        
        if llm_r.get("rejected"):
            await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                             horas_raw,None,None,False,False,False,None,conf_llm,"","error","LLM_REJECTED",llm_r.get("parseo_ms",0))
            s.timer_end("fichaje")
            if pf.get("pat")=="kw":
                return "Dime las horas de entrada y salida. Ej: 9-17 \U0001f550"
            return None  # Not a fichaje, route to general
        if llm_r.get("needs_clarification"):
            ent_llm=llm_r.get("entrada","?");sal_llm=llm_r.get("salida","?")
            await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                             horas_raw,ent_llm,sal_llm,False,False,True,None,conf_llm,"","aclaracion","MEDIUM_CONFIDENCE",llm_r.get("parseo_ms",0))
            # Save espera so "si" gets routed back to fichaje confirmation
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"confirmar_fichaje","dominio":"FICHAJE","contexto":{"entrada":ent_llm,"salida":sal_llm,"mensaje_original":texto}})
            s.timer_end("fichaje")
            return f"Creo que has dicho {ent_llm} a {sal_llm}, es correcto? Responde si o repite las horas \U0001f550"
        
        if not llm_r.get("es_fichaje") or not llm_r.get("ok"):
            await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                             horas_raw,None,None,False,False,False,None,conf_llm,"","aclaracion","NEEDS_HOURS",llm_r.get("parseo_ms",0))
            s.timer_end("fichaje")
            if pf.get("pat")=="kw":
                return "Dime las horas de entrada y salida. Ej: 9-17 \U0001f550"
            return None  # Not a fichaje — return None to let router handle as general
        
        entrada=llm_r.get("entrada");salida=llm_r.get("salida")
        overnight=llm_r.get("overnight",False);inferred_pm=llm_r.get("inferred_pm",False)
        dur=llm_r.get("dur",0)
        log.info(f"[{s.trace_id}] LLM OK: {entrada}-{salida} {dur}h conf={conf_llm}")
    
    elif pf.get("det") and pf.get("error_code"):
        # Regex detected but invalid
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,"regex",
                         horas_raw,None,None,False,False,False,None,None,"","error",pf.get("error_code","PARSE_ERROR"),int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        return f"No entendi las horas. Formato: 9-17 o de 9 a 17 \U0001f550"
    
    else:
        # Not a fichaje at all
        s.timer_end("fichaje"); return None
    
    if not entrada or not salida:
        s.timer_end("fichaje"); return "No pude extraer entrada y salida. Repite: 9-17 \U0001f550"
    
    # ═══ STEP 3: Idempotencia ═══
    sig_provisional=generar_signature(emp_id,hoy,entrada,salida)
    is_dup=await check_idempotencia(sig_provisional,provisional=True)
    if is_dup:
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                         horas_raw,entrada,salida,overnight,inferred_pm,False,dur,conf_llm,sig_provisional,"duplicado",None,int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        log.info(f"[{s.trace_id}] Fichaje duplicado: sig={sig_provisional}")
        return "Ya tenia registrado ese fichaje \U0001f44d"
    
    # ═══ STEP 4: Register via Python fichajes backend ═══
    # Send NORMALIZED hours to backend (prevents backend re-parsing "8 a 6" as -2h)
    msg_para_backend=f"de {entrada} a {salida}" if entrada and salida else texto
    body={"mensaje":msg_para_backend,"empleado_id":emp_id,"empleado_nombre":s.empleado.get("nombre",""),
          "empleado_telefono":s.empleado.get("telefono",""),"coste_hora":s.empleado.get("coste_hora",0),
          "fuera_madrid_hora":15}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(f"{PYTHON_URL}/procesar-fichaje",json=body)
            d=safe_json_response(r,f"[{s.trace_id}] fichaje_procesar",{})
            if d.get("_parse_error"):
                raise ValueError(f"backend fichajes devolvio respuesta no JSON ({d.get('_http_status')})")
        msg=d.get("mensaje",d.get("message",str(d)))
        resultado="registrado"
        
        if d.get("error") or "error" in str(d).lower()[:50]:
            resultado="error"
            if s.dominio_fuente=="espera":
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
        
        if "obra" in msg.lower() and "1." in msg:
            log.info(f"[{s.trace_id}] Saving espera seleccion_obra with original msg")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"ok":True,"mensaje_original":msg_para_backend}})
        elif "?" in msg:
            log.info(f"[{s.trace_id}] Backend asked question, saving espera continuation")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"continuation":True,"mensaje_original":msg_para_backend}})
        
        # ═══ STEP 5: Log detallado ═══
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                         horas_raw,entrada,salida,overnight,inferred_pm,False,dur,conf_llm,sig_provisional,resultado,None,int((time.time()-t0)*1000))
        s.timer_end("fichaje"); return msg
    except Exception as e:
        s.add_error(f"Fichaje: {e}")
        await log_fichaje(s.trace_id,emp_id,s.telefono,s.mensaje_original,texto,patron,metodo,
                         horas_raw,entrada,salida,overnight,inferred_pm,False,dur,conf_llm,sig_provisional,"error",str(e)[:200],int((time.time()-t0)*1000))
        s.timer_end("fichaje")
        if s.dominio_fuente=="espera":
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
        return "Problema registrando el fichaje \U0001f527 Repite tus horas (ej: 9-17)"


# ══════════════ AGENTE NÓMINA ══════════════
MESES={"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}

def parsear_nomina(texto,empleado_nombre=""):
    """Parse employee name, month and year from nomina request"""
    t=texto.lower()
    mes=None
    for nombre_mes,num in MESES.items():
        if nombre_mes in t: mes=num;break
    if not mes:
        m2=re.search(r'mes\s*(\d{1,2})',t)
        if m2: mes=int(m2.group(1))
    if not mes: mes=datetime.now().month if datetime.now().day>10 else (datetime.now().month-1 or 12)
    m2=re.search(r'20(\d{2})',t)
    anio=int(f"20{m2.group(1)}") if m2 else datetime.now().year
    if mes>datetime.now().month and anio==datetime.now().year: anio-=1
    nombre=None
    m2=re.search(r'(?:n[oó]mina|sueldo|debo|pagar)\s+(?:de|a|al)\s+([A-ZÁÉÍÓÚa-záéíóú]+(?:\s+[A-ZÁÉÍÓÚa-záéíóú]+)*)',texto)
    if m2: nombre=m2.group(1).strip()
    if not nombre or nombre.lower() in ("mi","me","yo","este mes","enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"):
        nombre=empleado_nombre
    return {"empleado_nombre":nombre,"mes":mes,"anio":anio}

async def enviar_doc_whatsapp(telefono,drive_file_id,filename):
    """Download PDF from Drive via n8n and send via WhatsApp Evolution sendMedia"""
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            # Download from Drive via n8n webhook
            dr=await c.post(f"{N8N}/webhook/download-drive-base64",json={"drive_file_id":drive_file_id,"filename":filename})
            dd=safe_json_response(dr,"download_drive_base64",{})
        if not dd.get("success") or not dd.get("base64"):
            log.error(f"Drive download failed: {str(dd)[:200]}")
            return False
        # Send via Evolution sendMedia
        body={"number":f"{telefono}@s.whatsapp.net","mediatype":"document","mimetype":"application/pdf","media":dd["base64"],"fileName":filename if filename.lower().endswith(".pdf") else f"{filename}.pdf","caption":f"\U0001f4c4 {filename}"}
        async with httpx.AsyncClient(timeout=30) as c:
            await c.post(f"{EVO}/message/sendMedia/{INSTANCE}",headers={"apikey":EK,"Content-Type":"application/json"},json=body)
        return True
    except Exception as e:
        log.error(f"enviar_doc_whatsapp: {e}")
        return False

async def ag_nomina(s):
    """Nómina: cálculo (admin/encargado) o envío PDF (empleados con DNI verification)"""
    s.timer_start("nomina")
    texto=s.mensaje_normalizado.lower()
    rol=int(s.empleado.get("rol_id",s.empleado.get("rol",99)) or 99)
    datos=parsear_nomina(s.mensaje_normalizado,s.empleado.get("nombre",""))
    log.info(f"[{s.trace_id}] Nomina: {datos} rol={rol}")
    await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="nomina",empleado=datos.get("empleado_nombre",""),mes=_ctx_mes_txt(datos.get("mes"),datos.get("anio")),paso="consulta")
    
    # Detect if asking for PDF document vs calculation
    quiere_pdf=any(w in texto for w in ["manda","envia","envía","pdf","documento","descarga","dame"])
    
    # Check if asking for own or another's
    mi_nombre=s.empleado.get("nombre","").lower().strip()
    nombre_pedido=datos.get("empleado_nombre","").lower().strip()
    def _norm(n):
        p=n.split()
        return (p[0],p[-1]) if len(p)>=2 else (p[0],"") if p else ("","")
    es_propia=_norm(mi_nombre)==_norm(nombre_pedido) or (_norm(mi_nombre)[0]==_norm(nombre_pedido)[0])
    
    # ACCESS CONTROL
    if rol==1:
        pass  # Admin: full access
    elif rol==2:
        if not es_propia:
            target=await db_get("empleados",f"nombre=ilike.*{datos['empleado_nombre'].split()[0]}*&select=rol_id&limit=1")
            if target and int(target[0].get("rol_id",0) or 0)==1:
                s.timer_end("nomina");return "No tienes acceso a esa nomina \U0001f512"
    else:
        # Operario: only own, needs DNI for PDF
        if not es_propia:
            s.timer_end("nomina");return "Solo puedes consultar tu propia nomina \U0001f512"
        if quiere_pdf:
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"nomina_dni","dominio":"NOMINA","contexto":{"datos_nomina":datos,"quiere_pdf":True}})
            s.timer_end("nomina");return "Para enviarte la nomina necesito verificar tu identidad. Dime tu DNI/NIE \U0001f4cb"
    
    if quiere_pdf:
        # Send PDF directly (admin/encargado, or after DNI verification)
        return await _buscar_y_enviar_nomina_pdf(s,datos,s.telefono)
    else:
        # Calculate nómina
        return await _calcular_nomina(s,datos)

async def _calcular_nomina(s,datos):
    """Call Python fichajes backend to calculate nómina"""
    try:
        log.info(f"[{s.trace_id}] Calcular nomina: {datos}")
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(f"{PYTHON_URL}/calcular-nomina",json=datos)
            d=safe_json_response(r,f"[{s.trace_id}] nomina_calcular",{})
        if d.get("_parse_error"):
            raise ValueError(f"backend nomina devolvio respuesta no JSON ({d.get('_http_status')})")
        if d.get("success"):
            s.timer_end("nomina");return d.get("resumen","Nomina calculada pero sin resumen")
        else:
            s.timer_end("nomina");return d.get("mensaje","") or f"No pude calcular la nomina \U0001f527"
    except Exception as e:
        log.error(f"Nomina calc: {e}");s.add_error(f"Nomina: {e}");s.timer_end("nomina")
        return f"Problema calculando la nomina \U0001f527"

async def _buscar_y_enviar_nomina_pdf(s,datos,telefono_destino):
    """Search documentos table and send PDF via WhatsApp"""
    emp_nombre=datos.get("empleado_nombre","")
    mes_num=datos.get("mes",0);anio=datos.get("anio",2026)
    MESES_ABR={1:"ENE",2:"FEB",3:"MAR",4:"ABR",5:"MAY",6:"JUN",7:"JUL",8:"AGO",9:"SEP",10:"OCT",11:"NOV",12:"DIC"}
    mes_abr=MESES_ABR.get(mes_num,"")
    anio_short=str(anio)[-2:]
    # Find empleado_id — use own ID if asking for own nómina, otherwise search
    mi_n2=s.empleado.get("nombre","").lower().strip()
    ped_n2=emp_nombre.lower().strip()
    if ped_n2 in mi_n2 or mi_n2 in ped_n2 or ped_n2.split()[0] in mi_n2:
        emp_id=s.empleado.get("id",0)
    else:
        emp=await db_get("empleados",f"nombre=ilike.*{emp_nombre.split()[0]}*&select=id&limit=5")
        emp_id=emp[0]["id"] if emp else 0
    # Query documentos
    q=f"empleado_id=eq.{emp_id}&tipo_documento=eq.NOMINA"
    if mes_abr:q+=f"&mes=eq.{mes_abr}"
    if anio_short:q+=f"&anio=eq.{anio_short}"
    q+="&order=created_at.desc&limit=1&select=drive_file_id,nombre_archivo,mes,anio"
    docs=await db_get("documentos",q)
    log.info(f"[{s.trace_id}] Doc query: {q} → {len(docs)} results")
    if not docs:
        s.timer_end("nomina");return f"No encontre la nomina de {mes_abr}-{anio_short} para {emp_nombre} \U0001f4cb"
    doc=docs[0]
    # Send PDF
    ok=await enviar_doc_whatsapp(telefono_destino,doc["drive_file_id"],doc.get("nombre_archivo","nomina.pdf"))
    s.timer_end("nomina")
    if ok:return f"\u2705 Te he enviado la nomina de {doc.get('mes','')}-{doc.get('anio','')} \U0001f4c4"
    else:return f"No pude enviar el documento \U0001f527 Contacta con administracion"

# ══════════════ OTROS AGENTES ══════════════
async def ag_saludo(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre","compañero")
    nt2=s.empleado.get("notas_bia","")
    return await gpt(f'El empleado {n} te saluda: "{s.mensaje_normalizado}". Responde un saludo corto y cercano.',BIA_PERSONA+f" Hablas con: {n}. Notas: {nt2}. Max 2 lineas.","gpt-4o-mini",150) or f"¡Buenas, {n}! Dime \U0001f4aa"

async def ag_obras(s):
    s.timer_start("obras");obras=await db_get("obras","select=id,nombre,estado,direccion,presupuesto_total,spreadsheet_id&estado=eq.En curso&order=nombre")
    n=s.empleado.get("apodo") or s.empleado.get("nombre","")
    hist=await cargar_historial(s.telefono)
    saludar=debe_saludar(s.mensaje_normalizado,hist)
    inst="Si la conversacion ya esta en marcha, contesta sin saludo ni introduccion. Ve directa al dato."
    if es_cortesia_simple(s.mensaje_normalizado):
        inst="Es una respuesta de cortesia. Contesta muy corto, amable y sin abrir un tema nuevo ni saludar."
    r2=await gpt(f'Obras en curso:\n{json.dumps(obras,ensure_ascii=False)[:2000]}\n\nPregunta: "{s.mensaje_normalizado}"\nCorto para WhatsApp.',BIA_PERSONA+f" Hablas con {n}. {inst}","gpt-4o")
    if r2 and not saludar:
        r2=quitar_saludo_repetido(r2)
    s.timer_end("obras"); return r2 or "No pude consultar obras. \U0001f527"

async def ag_finanzas(s):
    s.timer_start("finanzas");g2=await db_get("gastos","select=id,concepto,total,obra,proveedor&order=created_at.desc&limit=10")
    hist=await cargar_historial(s.telefono)
    saludar=debe_saludar(s.mensaje_normalizado,hist)
    inst="No saludes si ya estabais hablando. Responde natural y directa."
    if es_cortesia_simple(s.mensaje_normalizado):
        inst="Es una respuesta de cortesia. Contesta muy corto, amable y sin saludar."
    r2=await gpt(f'Gastos recientes:\n{json.dumps(g2,ensure_ascii=False)[:1500]}\n\nPregunta: "{s.mensaje_normalizado}"',f"{BIA_PERSONA} {inst}","gpt-4o")
    if r2 and not saludar:
        r2=quitar_saludo_repetido(r2)
    s.timer_end("finanzas"); return r2 or "No pude consultar finanzas. \U0001f527"

async def ag_empleados(s):
    s.timer_start("emps");e2=await db_get("empleados","select=id,nombre,cargo,estado&estado=eq.Activo&order=nombre")
    hist=await cargar_historial(s.telefono)
    saludar=debe_saludar(s.mensaje_normalizado,hist)
    inst="No saludes si ya estabais hablando. Responde como continuacion natural."
    if es_cortesia_simple(s.mensaje_normalizado):
        inst="Es una respuesta de cortesia. Contesta muy corto, amable y sin saludar."
    r2=await gpt(f'Empleados:\n{json.dumps(e2,ensure_ascii=False)[:1500]}\n\nPregunta: "{s.mensaje_normalizado}"',f"{BIA_PERSONA} {inst}","gpt-4o")
    if r2 and not saludar:
        r2=quitar_saludo_repetido(r2)
    s.timer_end("emps"); return r2 or "No pude consultar empleados. \U0001f527"

async def ag_general(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre","")
    nt2=s.empleado.get("notas_bia","")
    hist=await cargar_historial(s.telefono)
    ctx=await cargar_contexto_resumido(s.telefono)
    hechos=await cargar_memoria_hechos(s.telefono,8)
    resumenes=await cargar_resumenes_dialogo(s.telefono,3)
    saludar=debe_saludar(s.mensaje_normalizado,hist)
    h="".join([("Emp: " if m.get("role")=="user" else "Bia: ")+m.get("content","")+"\n" for m in hist[-8:]]) if hist else ""
    ctx_txt=", ".join([f"{k}: {v}" for k,v in ctx.items() if k!="updated_at"]) if ctx else "ninguno"
    hechos_txt="\n".join([f"- {x.get('tema')}/{x.get('clave')}: {x.get('valor')}" for x in hechos]) if hechos else "- sin hechos guardados"
    res_txt="\n".join([f"- {r.get('tema') or 'tema'}: {r.get('resumen','')}" for r in resumenes]) if resumenes else "- sin resumenes"
    prompt_sistema=BIA_PERSONA+f"\nHablas con: {n}\nNotas: {nt2}\nContexto activo: {ctx_txt}\nMemoria larga:\n{hechos_txt}\nResumenes recientes:\n{res_txt}\nHistorial:\n{h}\nSi el mensaje encaja con el contexto activo, continua ese tema. Si usa referencias como 'esa obra', 'el de antes' o 'marzo', apóyate primero en el contexto activo y luego en la memoria larga. Si cambia de tema claramente, cambia sin arrastrar el contexto viejo."
    out=await gpt(s.mensaje_normalizado,prompt_sistema,"gpt-4o") or f"Perdona {n}, no te entendi \U0001f914"
    if out and not saludar:
        out=quitar_saludo_repetido(out)
    return out

async def ag_obra_alta(s):
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_nombre","dominio":"OBRA_ALTA","contexto":{}})
    return "Vamos a abrir una obra! \U0001f3d7\ufe0f Como se llama?"

async def ag_obra_baja(s):
    obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
    lista="\n".join([f"{i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_baja","dominio":"OBRA_BAJA","contexto":{"obras_ids":[o["id"] for o in obras]}})
    return f"Que obra quieres cerrar?\n\n{lista}\n\nDime numero o nombre"

async def ag_reabrir_obra(s):
    anio = datetime.now().strftime("%Y")
    obras = await db_get("obras", f"estado=eq.Cerrada&select=id,nombre&order=nombre")
    if not obras: return "No hay obras cerradas"
    lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "reabrir_obra_sel", "dominio": "INTENT", "contexto": {"obras_ids": [o["id"] for o in obras], "obras_nombres": [o["nombre"] for o in obras]}})
    return f"Que obra quieres REABRIR?\n\n{lista}\n\nDime numero o nombre"

async def ag_cerrar_obra_cmd(s):
    obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
    if not obras: return "No hay obras activas"
    lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "obra_baja", "dominio": "OBRA_BAJA", "contexto": {"obras_ids": [o["id"] for o in obras]}})
    return f"Que obra quieres CERRAR?\n\n{lista}\n\nDime numero o nombre"

async def ag_alta_empleado(s):
    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_nombre", "dominio": "INTENT", "contexto": {}})
    return "Vamos a dar de alta un empleado \U0001f477\n\n1/6 Nombre completo?"

async def ag_baja_empleado(s):
    emps = await db_get("empleados", "estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados activos"
    lista = "\n".join([f"  {i+1}. *{e['nombre']}*" for i, e in enumerate(emps)])
    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "baja_empleado_sel", "dominio": "INTENT", "contexto": {"emps_ids": [e["id"] for e in emps], "emps_nombres": [e["nombre"] for e in emps]}})
    return f"Que empleado quieres dar de BAJA?\n\n{lista}\n\nDime numero o nombre"

async def ag_ayuda_admin(s):
    return """\U0001f6e0 *COMANDOS ADMIN (MAYUSCULAS EXACTAS):*

\U0001f3d7 *ALTA OBRA* \u2014 Crear obra nueva (4 pasos)
\U0001f512 *CERRAR OBRA* \u2014 Cerrar obra activa
\U0001f504 *REABRIR OBRA* \u2014 Reabrir obra cerrada
\U0001f477 *ALTA EMPLEADO* \u2014 Dar de alta (6 pasos)
\u274c *BAJA EMPLEADO* \u2014 Dar de baja empleado
\u2753 *AYUDA* \u2014 Ver esta lista

\U0001f4a1 _Escribe el comando EXACTO en mayusculas_"""

async def ag_ayuda_emp(s):
    rol = int(s.empleado.get("rol_id", 99) or 99)
    txt = """\U0001f916 *Que puedo hacer por ti:*

\U0001f552 *Fichajes*
  \u2022 Manda tus horas: _9-17_ o _de 8 a 18_
  \u2022 _he fichado hoy?_
  \u2022 _cuantas horas llevo este mes?_
  \u2022 _mis fichajes de hoy_

\U0001f9fe *Facturas*
  \u2022 Manda foto de factura o ticket

\U0001f4b6 *Nominas*
  \u2022 _enviame mi nomina de febrero_
  \u2022 _calcula mi nomina_

\U0001f4cb *Info*
  \u2022 _en que obra estoy?_
  \u2022 _mis anticipos_
  \u2022 _cuanto he ganado este mes?_

\U0001f3a4 *Audio* \u2014 Manda nota de voz"""
    if rol <= 2:
        txt += """

\U0001f465 *Equipo* (admin/encargado)
  \u2022 _quien ficho hoy?_
  \u2022 _resumen de hoy_
  \u2022 _gastos de la obra_
  \u2022 _cuanto gasto Nistor hoy?_
  \u2022 _horas en la obra esta semana_

"""
    return txt

# ═══ COMANDOS PASO A PASO ═══
async def ag_cmd_horas_obra(s):
    obras=await db_get("obras","estado=eq.En curso&select=id,nombre&order=nombre")
    if not obras: return "No hay obras activas"
    await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="horas_obra",paso="elegir_obra")
    lista="\n".join([f"  {i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_horas_obra_1","dominio":"CMD","contexto":{"obras":[{"id":o["id"],"nombre":o["nombre"]} for o in obras]}})
    return f"\U0001f552 *HORAS OBRA*\n\nQue obra?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_gastos_emp(s):
    emps=await db_get("empleados","estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados activos"
    await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="gastos_empleado",paso="elegir_empleado")
    lista="\n".join([f"  {i+1}. *{e['nombre']}*" for i,e in enumerate(emps)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_emp_1","dominio":"CMD","contexto":{"emps":[{"id":e["id"],"nombre":e["nombre"]} for e in emps]}})
    return f"\U0001f4b8 *GASTOS EMPLEADO*\n\nQue empleado?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_gastos_obra(s):
    obras=await db_get("obras","estado=eq.En curso&select=id,nombre&order=nombre")
    if not obras: return "No hay obras activas"
    await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="gastos_obra",paso="elegir_obra")
    lista="\n".join([f"  {i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_obra_1","dominio":"CMD","contexto":{"obras":[{"id":o["id"],"nombre":o["nombre"]} for o in obras]}})
    return f"\U0001f4b0 *GASTOS OBRA*\n\nQue obra?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_calc_nomina(s):
    emps=await db_get("empleados","estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados activos"
    lista="\n".join([f"  {i+1}. *{e['nombre']}*" for i,e in enumerate(emps)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_calc_nom_1","dominio":"CMD","contexto":{"emps":[{"id":e["id"],"nombre":e["nombre"]} for e in emps]}})
    return f"\U0001f4ca *CALCULAR NOMINA*\n\nQue empleado?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_enviar_nomina(s):
    emps=await db_get("empleados","estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados"
    lista="\n".join([f"  {i+1}. *{e['nombre']}*" for i,e in enumerate(emps)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_enviar_nom_1","dominio":"CMD","contexto":{"emps":[{"id":e["id"],"nombre":e["nombre"]} for e in emps]}})
    return f"\U0001f4c4 *ENVIAR NOMINA*\n\nA que empleado?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_anticipo(s):
    emps=await db_get("empleados","estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados"
    lista="\n".join([f"  {i+1}. *{e['nombre']}*" for i,e in enumerate(emps)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_anticipo_1","dominio":"CMD","contexto":{"emps":[{"id":e["id"],"nombre":e["nombre"]} for e in emps]}})
    return f"\U0001f4b6 *ANTICIPO*\n\nQue empleado?\n\n{lista}\n\nDime numero o nombre"

async def ag_cmd_reg_anticipo(s):
    emps=await db_get("empleados","estado=eq.Activo&select=id,nombre&order=nombre")
    if not emps: return "No hay empleados"
    lista="\n".join([f"  {i+1}. *{e['nombre']}*" for i,e in enumerate(emps)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_reg_ant_1","dominio":"CMD","contexto":{"emps":[{"id":e["id"],"nombre":e["nombre"]} for e in emps]}})
    return f"\U0001f4b3 *REGISTRAR ANTICIPO*\n\nQue empleado?\n\n{lista}\n\nDime numero o nombre"

AG={"FICHAJE":ag_fichaje,"OBRA_ALTA":ag_obra_alta,"OBRA_BAJA":ag_obra_baja,"SALUDO":ag_saludo,"OBRAS":ag_obras,"FINANZAS":ag_finanzas,"EMPLEADOS":ag_empleados,"NOMINA":ag_nomina,"DOCUMENTOS":ag_general,"INVENTARIO":ag_general,"GENERAL":ag_general,"AYUDA":ag_ayuda_admin,"AYUDA_EMP":ag_ayuda_emp,"REABRIR_OBRA":ag_reabrir_obra,"CERRAR_OBRA":ag_cerrar_obra_cmd,"ALTA_EMPLEADO":ag_alta_empleado,"BAJA_EMPLEADO":ag_baja_empleado,"CMD_HORAS_OBRA":ag_cmd_horas_obra,"CMD_GASTOS_EMP":ag_cmd_gastos_emp,"CMD_GASTOS_OBRA":ag_cmd_gastos_obra,"CMD_CALC_NOMINA":ag_cmd_calc_nomina,"CMD_ENVIAR_NOMINA":ag_cmd_enviar_nomina,"CMD_ANTICIPO":ag_cmd_anticipo,"CMD_REG_ANTICIPO":ag_cmd_reg_anticipo}

# ══════════════ ROUTER PRINCIPAL ══════════════
async def procesar(s):
    t0=time.time();s.trace_id=str(uuid.uuid4())[:8]
    s.mensaje_normalizado=normalizar_horas(s.mensaje_original.strip())
    rol_actual=int(s.empleado.get("rol_id",s.empleado.get("rol",99)) or 99)
    log.info(f"[{s.trace_id}] \U0001f4e9 {s.empleado.get('nombre','?')}: {s.mensaje_original[:80]}")
    # ═══ MENU SYSTEM — role-based, bilingual, numeric ═══
    _cmd = s.mensaje_original.strip()
    _cmd_low = _cmd.lower()
    if _cmd_low in ("ayuda","help","menu","ajutor"):
        _rol=rol_actual
        _idioma=s.empleado.get("idioma","es") or "es"
        if _cmd_low == "ajutor": _idioma = "ro"
        _nombre=s.empleado.get("apodo") or s.empleado.get("nombre","")
        s.dominio="MENU";s.dominio_fuente="cmd"
        
        if _rol == 1:
            # ADMIN: 18 options (12 admin + 6 personal)
            s.respuesta=f"\U0001f6e0 Hola {_nombre}! Menu admin:\n\n*ADMIN:*\n1. ALTA OBRA\n2. CERRAR OBRA\n3. REABRIR OBRA\n4. ALTA EMPLEADO\n5. BAJA EMPLEADO\n6. HORAS OBRA\n7. GASTOS EMPLEADO\n8. GASTOS OBRA\n9. CALCULAR NOMINA\n10. ENVIAR NOMINA\n11. ANTICIPO\n12. REGISTRAR ANTICIPO\n\n*PERSONAL:*\n13. Fichar\n14. Mi nomina\n15. Cuanto cobro\n16. Horas trabajadas\n17. Mis anticipos\n18. Datos empresa\n\nDime numero o comando \U0001f60a"
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"menu_admin","dominio":"MENU","contexto":{"idioma":_idioma}})
        
        elif _rol == 2:
            # ENCARGADO: 14 options (7 equipo + 7 personal) — sin BAJA EMP, CALC/ENVIAR NOM, ANTICIPO, REG ANTICIPO
            if _idioma == "ro":
                s.respuesta=f"\U0001f477 Buna {_nombre}!\n\n*ECHIPA:*\n1. ALTA OBRA\n2. CERRAR OBRA\n3. REABRIR OBRA\n4. ALTA EMPLEADO\n5. HORAS OBRA\n6. GASTOS EMPLEADO\n7. GASTOS OBRA\n\n*PERSONAL:*\n8. Pontaj\n9. Calculeaza salariu\n10. Cat castig\n11. Ore lucrate\n12. Trimite-mi Nomina\n13. Avansurile mele\n14. Date firma\n15. Encargado meu\n\nSpune-mi numarul \U0001f60a"
            else:
                s.respuesta=f"\U0001f477 Hola {_nombre}!\n\n*EQUIPO:*\n1. ALTA OBRA\n2. CERRAR OBRA\n3. REABRIR OBRA\n4. ALTA EMPLEADO\n5. HORAS OBRA\n6. GASTOS EMPLEADO\n7. GASTOS OBRA\n\n*PERSONAL:*\n8. Fichar\n9. Calcular nomina\n10. Cuanto cobro\n11. Horas trabajadas\n12. Enviame mi nomina\n13. Mis anticipos\n14. Datos empresa\n15. Mi encargado\n\nDime numero o comando \U0001f60a"
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"menu_enc","dominio":"MENU","contexto":{"idioma":_idioma}})
        
        else:
            # EMPLEADO: 8 options
            if _idioma == "ro":
                s.respuesta=f"\U0001f916 Buna {_nombre}! Ce ai nevoie?\n\n1. Pontaj\n2. Calculeaza salariu\n3. Cat castig\n4. Ore lucrate\n5. Trimite-mi Nomina\n6. Avansurile mele\n7. Date firma\n8. Encargado meu\n\nSpune-mi numarul \U0001f60a sau scrie-mi!"
            else:
                s.respuesta=f"\U0001f916 Hola {_nombre}! Que necesitas?\n\n1. Fichar\n2. Calcular nomina\n3. Cuanto cobro\n4. Horas trabajadas\n5. Enviame mi nomina\n6. Mis anticipos\n7. Datos empresa\n8. Mi encargado\n\nDime el numero \U0001f60a o escribeme!"
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"menu_emp","dominio":"MENU","contexto":{"idioma":_idioma}})
        
        s.duracion_ms=int((time.time()-t0)*1000)
        if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
        await guardar_ejecucion(s);return s
    # ═══ CMD EXACTOS MAYUSCULAS (before esperas and intents) ═══
    _cmd_map={"HORAS OBRA":"CMD_HORAS_OBRA","GASTOS EMPLEADO":"CMD_GASTOS_EMP","GASTOS OBRA":"CMD_GASTOS_OBRA","CALCULAR NOMINA":"CMD_CALC_NOMINA","ENVIAR NOMINA":"CMD_ENVIAR_NOMINA","ANTICIPO":"CMD_ANTICIPO","REGISTRAR ANTICIPO":"CMD_REG_ANTICIPO","REABRIR OBRA":"REABRIR_OBRA","CERRAR OBRA":"CERRAR_OBRA","ALTA EMPLEADO":"ALTA_EMPLEADO","BAJA EMPLEADO":"BAJA_EMPLEADO","ALTA OBRA":"OBRA_ALTA"}
    if _cmd in _cmd_map:
        s.dominio=_cmd_map[_cmd];s.dominio_fuente="cmd";s.accion="paso1"
        log.info(f"[{s.trace_id}] \U0001f6e0 CMD: {_cmd} -> {s.dominio}")
        s.timer_start("agente")
        try:s.respuesta=await AG.get(s.dominio,ag_general)(s)
        except Exception as e:s.add_error(f"CMD: {e}");s.respuesta=f"Error: {str(e)[:100]} \U0001f527"
        s.timer_end("agente");s.duracion_ms=int((time.time()-t0)*1000)
        if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
        await guardar_ejecucion(s);return s
    
    # Espera activa
    esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&order=created_at.desc&limit=1")
    if esperas:
        esp=esperas[0];log.info(f"[{s.trace_id}] \u23f3 Espera: {esp['tipo']}")
        if espera_caducada(esp):
            log.info(f"[{s.trace_id}] \u231b Espera caducada ({esp.get('tipo')}) > {ESPERA_TTL_MINUTES} min, borrando")
            await borrar_espera(esp["id"])
            esperas=[]
        else:
            ctx=esp.get("contexto",{}) or {}
            consume_espera=debe_consumir_espera(esp,s.mensaje_normalizado,rol_actual)
            if consume_espera:
                await borrar_espera(esp["id"])
            else:
                log.info(f"[{s.trace_id}] Manteniendo espera {esp.get('tipo')} para no mezclar flujos")
                esp={"tipo":"_skip_espera_","contexto":ctx}
        if not esperas:
            esp={"tipo":"_skip_espera_","contexto":{}}
            consume_espera=False
        # Factura obra selection
        if esp.get("tipo")=="factura_obra" and consume_espera:
            log.info(f"[{s.trace_id}] Factura obra selection: {s.mensaje_normalizado}")
            factura=ctx.get("factura",{})
            try:
                sel=int(s.mensaje_normalizado.strip())-1
                obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
                if 0<=sel<len(obras):
                    obra=obras[sel]
                    fdate=factura.get("fecha","2026-01-01")
                    try:
                        m2=int(fdate[5:7]);y2=fdate[:4];trim=f"T{(m2-1)//3+1}-{y2}"
                    except:trim="T1-2026"
                    gasto={"obra_id":obra["id"],"obra":obra["nombre"],"empleado_id":s.empleado.get("id",0),"empleado_nombre":s.empleado.get("nombre",""),"proveedor":factura.get("proveedor",""),"cif_proveedor":factura.get("CIF",factura.get("cif","")),"numero_factura":str(factura.get("numero_factura",factura.get("numero",""))),"fecha_factura":factura.get("fecha",None),"concepto":factura.get("concepto",""),"base_imponible":factura.get("base_imponible",0),"tipo_iva":factura.get("iva_porcentaje",21),"cuota_iva":factura.get("iva_importe",0),"irpf":0,"total":factura.get("total",0),"trimestre":trim,"drive_url":ctx.get("drive_url","")}
                    result=await db_post("gastos",gasto)
                    if "error" in str(result):log.error(f"Gastos: {result}")
                    prov=factura.get("proveedor","?");tot=factura.get("total",0)
                    try:
                        concepto=factura.get("concepto","")
                        if isinstance(concepto,list):concepto=", ".join(concepto)
                        sheet_data={"spreadsheet_id":(obra.get("spreadsheet_id","") or "").strip(),"obra_nombre":obra["nombre"],"obra_id":obra["id"],"proveedor":prov,"numero_factura":str(factura.get("numero_factura",factura.get("numero",""))),"cif":factura.get("CIF",factura.get("cif","")),"concepto":concepto,"base":factura.get("base_imponible",0),"iva":factura.get("iva_importe",0),"total":tot,"fecha":factura.get("fecha",""),"trimestre":trim,"empleado":s.empleado.get("nombre",""),"empleado_telefono":s.empleado.get("telefono",""),"drive_url":ctx.get("drive_url","")}
                        async with httpx.AsyncClient(timeout=15) as sc:await sc.post(f"{N8N}/webhook/escribir-gasto-sheet",json=sheet_data)
                    except Exception as e:log.error(f"Sheet: {e}")
                    s.respuesta=f"\u2705 Gasto registrado!\n\nProveedor: {prov}\nTotal: {tot}\u20ac\nObra: {obra['nombre']}\nTrimestre: {trim}\n\nGuardado en BD + Sheet \u2705"
                else:
                    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"factura_obra","dominio":"FACTURA","contexto":ctx})
                    s.respuesta="Numero no valido. Dime el numero correcto."
            except:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"factura_obra","dominio":"FACTURA","contexto":ctx})
                s.respuesta="No entendi. Dime el numero de la obra."
            s.dominio="FACTURA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        # Obra alta steps
        if esp.get("tipo")=="obra_nombre" and consume_espera:
            ctx["nombre"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_direccion","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="Direccion de la obra?";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_direccion" and consume_espera:
            ctx["direccion"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_madrid","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="Dentro o fuera de Madrid?";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_madrid" and consume_espera:
            ctx["fuera_madrid"]="fuera" in s.mensaje_normalizado.lower()
            encs=await db_get("empleados","select=id,nombre,rol_id&rol_id=in.(1,2)&estado=eq.Activo&order=nombre")
            lista="\n".join([f"{i+1}. {e2['nombre']}" + (" (Admin)" if e2.get('rol_id')==1 else "") for i,e2 in enumerate(encs)])
            ctx["encargados"]=[e2["id"] for e2 in encs]
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_encargado","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta=f"Quien es el encargado?\n\n{lista}\n\nDime numero o nombre";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_encargado" and consume_espera:
            encs=await db_get("empleados","select=id,nombre,rol_id&rol_id=in.(1,2)&estado=eq.Activo&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1;enc=encs[sel] if 0<=sel<len(encs) else encs[0]
            except:enc=next((e2 for e2 in encs if s.mensaje_normalizado.lower() in e2["nombre"].lower()),encs[0])
            try:
                obra_data={"nombre":ctx.get("nombre",""),"direccion":ctx.get("direccion",""),"tipo":"reforma","presupuesto":0,"telefono":s.empleado.get("telefono",""),"encargado_id":enc["id"],"encargado_nombre":enc["nombre"],"fuera_madrid":ctx.get("fuera_madrid",False)}
                async with httpx.AsyncClient(timeout=60) as oc:await oc.post(f"{N8N}/webhook/alta-obra",json=obra_data)
                nombre_o=ctx.get("nombre","");dir_o=ctx.get("direccion","");fm="Fuera de Madrid" if ctx.get("fuera_madrid") else "Madrid"
                s.respuesta=f"\u2705 Obra creada!\n\n\U0001f3d7 {nombre_o}\n\U0001f4cd {dir_o}\n\U0001f30d {fm}\n\U0001f477 Encargado: {enc['nombre']}\n\n\U0001f4c1 Carpeta Drive + Sheet creados"
            except Exception as e:log.error(f"WF-15: {e}");s.respuesta="Error creando la obra."
            s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_baja" and consume_espera:
            obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1;obra=obras[sel] if 0<=sel<len(obras) else None
            except:obra=None
            if obra:
                async with httpx.AsyncClient(timeout=15) as pc:await pc.patch(f"{SUPA}/rest/v1/obras?id=eq.{obra['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json={"estado":"Cerrada"})
                s.respuesta=f"\u2705 Obra *{obra['nombre']}* cerrada."
            else:s.respuesta="No encontre esa obra."
            s.dominio="OBRA_BAJA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        if esp.get("tipo")=="n8n_pending" and consume_espera:
            try:
                fwd={"data":{"key":{"remoteJid":f"{s.telefono}@s.whatsapp.net","fromMe":False},"message":{"conversation":s.mensaje_original}}}
                async with httpx.AsyncClient(timeout=30) as fc:await fc.post(N8N_WEBHOOK or f"{N8N}/webhook/whatsapp-euromir",json=fwd)
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":0,"tipo":"n8n_pending","dominio":"N8N","contexto":{"text":True}})
            except:pass
            s.dominio="N8N";s.dominio_fuente="espera";s.respuesta="";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        # General espera
        # Handle nomina_dni espera — employee verifying identity for nómina PDF
        if esp.get("tipo")=="nomina_dni" and consume_espera:
            dni_input=s.mensaje_normalizado.strip().upper().replace(" ","").replace("-","")
            emp_id=s.empleado.get("id",0)
            emp_data=await db_get("empleados",f"id=eq.{emp_id}&select=dni_nie,telefono,nombre")
            if emp_data:
                dni_db=(emp_data[0].get("dni_nie","") or "").strip().upper().replace(" ","").replace("-","")
                tel_db=(emp_data[0].get("telefono","") or "").strip()
                if dni_db and dni_input==dni_db and s.telefono==tel_db:
                    datos_nomina=ctx.get("datos_nomina",{"empleado_nombre":s.empleado.get("nombre",""),"mes":datetime.now().month,"anio":datetime.now().year})
                    s.timer_start("nomina")
                    s.respuesta=await _buscar_y_enviar_nomina_pdf(s,datos_nomina,s.telefono)
                elif not dni_db:
                    s.respuesta="No tienes DNI registrado en el sistema. Contacta con administracion \U0001f4cb"
                else:
                    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"nomina_dni","dominio":"NOMINA","contexto":ctx})
                    s.respuesta="DNI incorrecto \U0001f512"
            else:
                s.respuesta="No encontre tus datos \U0001f527"
            s.dominio="NOMINA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # Handle coste_obra_sel
        if esp.get("tipo") == "coste_obra_sel":
            obras_ids = ctx.get("obras_ids", []); obras_nombres = ctx.get("obras_nombres", [])
            obra_id, obra_nombre = None, None
            try:
                sel = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= sel < len(obras_ids): obra_id, obra_nombre = obras_ids[sel], obras_nombres[sel]
            except:
                for idx, nm in enumerate(obras_nombres):
                    if s.mensaje_normalizado.lower() in nm.lower(): obra_id, obra_nombre = obras_ids[idx], nm; break
            if not obra_id:
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "coste_obra_sel", "dominio": "INTENT", "contexto": ctx})
                s.respuesta = "No encontre esa obra. Dime el numero."; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
            facturas = await db_get("gastos", f"obra_id=eq.{obra_id}&select=total")
            fichajes = await db_get("fichajes_tramos", f"obra_id=eq.{obra_id}&select=coste_total")
            tf = round(sum(float(r.get("total", 0) or 0) for r in facturas), 2)
            tm = round(sum(float(r.get("coste_total", 0) or 0) for r in fichajes), 2)
            s.respuesta = f"\U0001f4b0 *Coste total de {obra_nombre}:*\n\n  \U0001f9fe Facturas: *{tf}\u20ac*\n  \U0001f477 Mano de obra: *{tm}\u20ac*\n  \u2500\u2500\u2500\n  \U0001f4b0 *TOTAL: {round(tf+tm,2)}\u20ac*"
            s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # Handle horas_obra_sel / empleados_obra_sel
        if esp.get("tipo") in ("horas_obra_sel", "empleados_obra_sel"):
            obras_ids = ctx.get("obras_ids", []); obras_nombres = ctx.get("obras_nombres", []); periodo = ctx.get("periodo", "todo")
            obra_id, obra_nombre = None, None
            try:
                sel = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= sel < len(obras_ids): obra_id, obra_nombre = obras_ids[sel], obras_nombres[sel]
            except:
                for idx, nm in enumerate(obras_nombres):
                    if s.mensaje_normalizado.lower() in nm.lower(): obra_id, obra_nombre = obras_ids[idx], nm; break
            if not obra_id:
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": esp.get("tipo"), "dominio": "INTENT", "contexto": ctx})
                s.respuesta = "No encontre esa obra. Dime el numero."; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
            # Build date filter
            from datetime import timedelta
            df = f"obra_id=eq.{obra_id}"
            per_txt = periodo
            if periodo == "hoy": df += f"&fecha=eq.{date.today().isoformat()}"
            elif periodo == "ayer": df += f"&fecha=eq.{(date.today()-timedelta(days=1)).isoformat()}"
            elif periodo == "semana":
                d = date.today(); inicio = (d - timedelta(days=d.weekday())).isoformat()
                df += f"&fecha=gte.{inicio}&fecha=lte.{d.isoformat()}"
            elif periodo == "mes": df += f"&fecha=gte.{date.today().strftime('%Y-%m-01')}"
            # else: todo (no date filter)
            
            if esp.get("tipo") == "horas_obra_sel":
                rows = await db_get("fichajes_tramos", f"{df}&select=empleado_nombre,horas_decimal,coste_total&order=empleado_nombre")
                if not rows:
                    s.respuesta = f"No hay fichajes en *{obra_nombre}* ({per_txt})"
                else:
                    # Group by cargo/category via empleado lookup
                    por_emp = {}
                    for r in rows:
                        nm = r.get("empleado_nombre", "?")
                        por_emp[nm] = por_emp.get(nm, 0) + float(r.get("horas_decimal", 0) or 0)
                    total_h = round(sum(por_emp.values()), 1)
                    det = "\n".join([f"  \U0001f477 *{nm}*: {round(h,1)}h" for nm, h in sorted(por_emp.items())])
                    s.respuesta = f"\U0001f552 *Horas en {obra_nombre} ({per_txt}):*\n\n{det}\n\n\u2500\u2500\u2500\n*TOTAL: {total_h}h*"
            else:
                rows = await db_get("fichajes_tramos", f"{df}&select=empleado_nombre,horas_decimal&order=empleado_nombre")
                if not rows:
                    s.respuesta = f"No hay fichajes en *{obra_nombre}* ({per_txt})"
                else:
                    por_emp = {}
                    for r in rows:
                        nm = r.get("empleado_nombre", "?")
                        por_emp[nm] = por_emp.get(nm, 0) + float(r.get("horas_decimal", 0) or 0)
                    det = "\n".join([f"  \U0001f477 *{nm}* \u2014 {round(h,1)}h" for nm, h in sorted(por_emp.items())])
                    s.respuesta = f"\U0001f477 *Empleados en {obra_nombre} ({per_txt}):*\n\n{det}\n\n*{len(por_emp)} empleados*"
            s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # ═══ ACCIONES ADMIN CON MAYÚSCULAS ═══
        # Handle reabrir_obra_sel
        if esp.get("tipo") == "reabrir_obra_sel":
            obras_ids = ctx.get("obras_ids", []); obras_nombres = ctx.get("obras_nombres", [])
            obra_id, obra_nombre = None, None
            try:
                sel = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= sel < len(obras_ids): obra_id, obra_nombre = obras_ids[sel], obras_nombres[sel]
            except:
                for idx, nm in enumerate(obras_nombres):
                    if s.mensaje_normalizado.lower() in nm.lower(): obra_id, obra_nombre = obras_ids[idx], nm; break
            if obra_id:
                await db_post("obras", {"id": obra_id})  # Can't patch via db_post, use direct
                async with httpx.AsyncClient(timeout=15) as c:
                    await c.patch(f"{SUPA}/rest/v1/obras?id=eq.{obra_id}", headers={"apikey": SK, "Authorization": f"Bearer {SK}", "Content-Type": "application/json", "Prefer": "return=representation"}, json={"estado": "En curso"})
                s.respuesta = f"\u2705 Obra *{obra_nombre}* reabierta!"
            else:
                s.respuesta = "No encontre esa obra."
            s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # Handle baja_empleado_sel
        if esp.get("tipo") == "baja_empleado_sel":
            emps_ids = ctx.get("emps_ids", []); emps_nombres = ctx.get("emps_nombres", [])
            emp_id_sel, emp_nombre = None, None
            try:
                sel = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= sel < len(emps_ids): emp_id_sel, emp_nombre = emps_ids[sel], emps_nombres[sel]
            except:
                for idx, nm in enumerate(emps_nombres):
                    if s.mensaje_normalizado.lower() in nm.lower(): emp_id_sel, emp_nombre = emps_ids[idx], nm; break
            if emp_id_sel:
                async with httpx.AsyncClient(timeout=15) as c:
                    await c.patch(f"{SUPA}/rest/v1/empleados?id=eq.{emp_id_sel}", headers={"apikey": SK, "Authorization": f"Bearer {SK}", "Content-Type": "application/json", "Prefer": "return=representation"}, json={"estado": "Baja"})
                s.respuesta = f"\u2705 *{emp_nombre}* dado de baja."
            else:
                s.respuesta = "No encontre ese empleado."
            s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # Handle alta_empleado steps
        if esp.get("tipo") and esp["tipo"].startswith("alta_emp_"):
            paso = esp["tipo"]
            val = s.mensaje_normalizado.strip()
            if paso == "alta_emp_nombre": ctx["nombre"] = val; await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_dni", "dominio": "INTENT", "contexto": ctx}); s.respuesta = "DNI/NIE?"; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000); await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta); await guardar_ejecucion(s); return s
            elif paso == "alta_emp_dni": ctx["dni"] = val; await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_tel", "dominio": "INTENT", "contexto": ctx}); s.respuesta = "Telefono?"; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000); await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta); await guardar_ejecucion(s); return s
            elif paso == "alta_emp_tel": ctx["telefono"] = val; await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_cat", "dominio": "INTENT", "contexto": ctx}); s.respuesta = "Categoria? (oficial/ayudante/encargado)"; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000); await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta); await guardar_ejecucion(s); return s
            elif paso == "alta_emp_cat": ctx["cargo"] = val; await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_email", "dominio": "INTENT", "contexto": ctx}); s.respuesta = "Email?"; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000); await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta); await guardar_ejecucion(s); return s
            elif paso == "alta_emp_email": ctx["email"] = val; await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "alta_emp_dir", "dominio": "INTENT", "contexto": ctx}); s.respuesta = "Direccion completa?"; s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000); await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta); await guardar_ejecucion(s); return s
            elif paso == "alta_emp_dir":
                ctx["direccion"] = val
                emp_data = {"nombre": ctx.get("nombre",""), "dni_nie": ctx.get("dni",""), "telefono": ctx.get("telefono",""), "cargo": ctx.get("cargo",""), "email": ctx.get("email",""), "direccion": ctx.get("direccion",""), "estado": "Activo", "rol_id": 3}
                result = await db_post("empleados", emp_data)
                if isinstance(result, list) or (isinstance(result, dict) and "error" not in str(result).lower()[:50]):
                    s.respuesta = f"\u2705 Empleado *{ctx.get('nombre','')}* dado de alta!\n\n\U0001f4cb DNI: {ctx.get('dni','')}\n\U0001f4de Tel: {ctx.get('telefono','')}\n\U0001f477 Cargo: {ctx.get('cargo','')}\n\U0001f4e7 Email: {ctx.get('email','')}\n\U0001f3e0 Dir: {ctx.get('direccion','')}"
                else:
                    s.respuesta = f"Error al crear empleado: {str(result)[:100]}"
                s.dominio = "INTENT"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
        
        # ═══ CMD STEP-BY-STEP HANDLERS ═══
        def _sel(items, txt, key="nombre"):
            try:
                idx=int(txt.strip())-1
                if 0<=idx<len(items): return items[idx]
            except:
                for item in items:
                    if txt.lower() in item.get(key,"").lower(): return item
            return None
        
        # HORAS OBRA: paso 1 (obra selected) → ask periodo
        if esp.get("tipo")=="cmd_horas_obra_1":
            sel=_sel(ctx.get("obras",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_horas_obra_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre esa obra. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="horas_obra",obra=sel["nombre"],paso="elegir_periodo")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_horas_obra_2","dominio":"CMD","contexto":{"obra_id":sel["id"],"obra_nombre":sel["nombre"]}})
            s.respuesta=f"Obra: *{sel['nombre']}*\n\nPeriodo?\n  1. Hoy\n  2. Ayer\n  3. Esta semana\n  4. Toda la obra\n\nDime numero"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # HORAS OBRA: paso 2 (periodo selected) → result
        if esp.get("tipo")=="cmd_horas_obra_2":
            obra_id=ctx.get("obra_id");obra_nombre=ctx.get("obra_nombre","?")
            t2=s.mensaje_normalizado.strip().lower()
            from datetime import date as date2, timedelta as td2
            df=f"obra_id=eq.{obra_id}"
            per="toda la obra"
            if t2 in("1","hoy"): df+=f"&fecha=eq.{date.today().isoformat()}";per="hoy"
            elif t2 in("2","ayer"): df+=f"&fecha=eq.{(date.today()-td2(days=1)).isoformat()}";per="ayer"
            elif t2 in("3","semana","esta semana"):
                d2=date.today();df+=f"&fecha=gte.{(d2-td2(days=d2.weekday())).isoformat()}&fecha=lte.{d2.isoformat()}";per="esta semana"
            rows=await db_get("fichajes_tramos",f"{df}&select=empleado_nombre,horas_decimal,coste_hora&order=empleado_nombre")
            if not rows:
                s.respuesta=f"No hay fichajes en *{obra_nombre}* ({per})"
            else:
                # Group by employee, get cargo from empleados
                por_emp={}
                for r in rows:
                    nm=(r.get("empleado_nombre") or "?");por_emp[nm]=por_emp.get(nm,0)+float(r.get("horas_decimal",0) or 0)
                # Get cargos
                emps_data=await db_get("empleados","estado=eq.Activo&select=nombre,cargo")
                cargo_map={(e.get("nombre","") or "").lower():(e.get("cargo","") or "Sin cargo") for e in emps_data}
                por_cargo={}
                for nm,h in por_emp.items():
                    cargo=cargo_map.get((nm or "?").lower(),"Otro")
                    por_cargo[cargo]=por_cargo.get(cargo,0)+h
                total=round(sum(por_emp.values()),1)
                det_emp="\n".join([f"  \U0001f477 *{nm}*: {round(h,1)}h" for nm,h in sorted(por_emp.items())])
                det_cargo="\n".join([f"  \U0001f3f7 *{c}*: {round(h,1)}h" for c,h in sorted(por_cargo.items())])
                s.respuesta=f"\U0001f552 *Horas en {obra_nombre} ({per}):*\n\n*Por empleado:*\n{det_emp}\n\n*Por categoria:*\n{det_cargo}\n\n*TOTAL: {total}h*"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # GASTOS EMPLEADO: paso 1 → ask periodo
        if esp.get("tipo")=="cmd_gastos_emp_1":
            sel=_sel(ctx.get("emps",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_emp_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="gastos_empleado",empleado=sel["nombre"],paso="elegir_periodo")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_emp_2","dominio":"CMD","contexto":{"emp_id":sel["id"],"emp_nombre":sel["nombre"]}})
            s.respuesta=f"Empleado: *{sel['nombre']}*\n\nPeriodo?\n  1. Hoy\n  2. Ayer\n  3. Esta semana\n  4. Este mes\n\nDime numero"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # GASTOS EMPLEADO: paso 2 → result
        if esp.get("tipo")=="cmd_gastos_emp_2":
            eid2=ctx.get("emp_id");enm=ctx.get("emp_nombre","?")
            t2=s.mensaje_normalizado.strip().lower()
            from datetime import date as date2, timedelta as td2
            hoy=date.today().isoformat()
            if t2 in("1","hoy"): f1,f2,per=f"{hoy}T00:00:00",f"{hoy}T23:59:59","hoy"
            elif t2 in("2","ayer"):
                ay=(date.today()-td2(days=1)).isoformat();f1,f2,per=f"{ay}T00:00:00",f"{ay}T23:59:59","ayer"
            elif t2 in("3","semana"):
                d2=date.today();ini=(d2-td2(days=d2.weekday())).isoformat();f1,f2,per=f"{ini}T00:00:00",f"{hoy}T23:59:59","esta semana"
            else:
                f1,f2,per=f"{date.today().strftime('%Y-%m')}-01T00:00:00",f"{hoy}T23:59:59","este mes"
            rows=await db_get("gastos",f"empleado_id=eq.{eid2}&created_at=gte.{f1}&created_at=lt.{f2}&select=proveedor,total,obra&order=created_at.desc")
            if not rows:
                s.respuesta=f"*{enm}* no tiene gastos {per}"
            else:
                total=round(sum(float(r.get("total",0) or 0) for r in rows),2)
                det="\n".join([f"  \U0001f9fe *{r.get('proveedor','?')}* — {r.get('total',0)}\u20ac ({r.get('obra','?')})" for r in rows])
                s.respuesta=f"\U0001f4b8 *Gastos de {enm} ({per}):*\n\n{det}\n\n*TOTAL: {total}\u20ac* ({len(rows)} facturas)"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # GASTOS OBRA: paso 1 → ask tipo
        if esp.get("tipo")=="cmd_gastos_obra_1":
            sel=_sel(ctx.get("obras",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_obra_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="gastos_obra",obra=sel["nombre"],paso="elegir_tipo")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_gastos_obra_2","dominio":"CMD","contexto":{"obra_id":sel["id"],"obra_nombre":sel["nombre"]}})
            s.respuesta=f"Obra: *{sel['nombre']}*\n\nQue quieres ver?\n  1. Solo facturas\n  2. Solo mano de obra\n  3. Todo (facturas + mano de obra)\n\nDime numero"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # GASTOS OBRA: paso 2 → result
        if esp.get("tipo")=="cmd_gastos_obra_2":
            oid=ctx.get("obra_id");onm=ctx.get("obra_nombre","?")
            t2=s.mensaje_normalizado.strip()
            fact=await db_get("gastos",f"obra_id=eq.{oid}&select=total")
            mo=await db_get("fichajes_tramos",f"obra_id=eq.{oid}&select=coste_total")
            tf=round(sum(float(r.get("total",0) or 0) for r in fact),2)
            tm=round(sum(float(r.get("coste_total",0) or 0) for r in mo),2)
            if t2 in("1","facturas"): s.respuesta=f"\U0001f9fe *Facturas en {onm}:* *{tf}\u20ac* ({len(fact)} facturas)"
            elif t2 in("2","mano"): s.respuesta=f"\U0001f477 *Mano de obra en {onm}:* *{tm}\u20ac*"
            else: s.respuesta=f"\U0001f4b0 *Gastos totales de {onm}:*\n\n  \U0001f9fe Facturas: *{tf}\u20ac*\n  \U0001f477 Mano de obra: *{tm}\u20ac*\n  \u2500\u2500\u2500\n  *TOTAL: {round(tf+tm,2)}\u20ac*"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # CALCULAR NOMINA: paso 1 → ask mes
        if esp.get("tipo")=="cmd_calc_nom_1":
            sel=_sel(ctx.get("emps",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_calc_nom_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="nomina",empleado=sel["nombre"],paso="elegir_mes")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_calc_nom_2","dominio":"CMD","contexto":{"emp_id":sel["id"],"emp_nombre":sel["nombre"]}})
            s.respuesta=f"Empleado: *{sel['nombre']}*\n\nQue mes? (ej: 1=enero, 2=febrero, 3=marzo...)"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # CALCULAR NOMINA: paso 2 → result
        if esp.get("tipo")=="cmd_calc_nom_2":
            enm=ctx.get("emp_nombre","?")
            try: mes=int(s.mensaje_normalizado.strip())
            except: mes=datetime.now().month
            anio=datetime.now().year
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="nomina",empleado=enm,mes=_ctx_mes_txt(mes,anio),paso="resultado")
            try:
                async with httpx.AsyncClient(timeout=30) as nc:
                    nr=await nc.post(f"{PYTHON_URL}/calcular-nomina",json={"empleado_nombre":enm,"mes":mes,"anio":anio})
                    nd=safe_json_response(nr,f"[{s.trace_id}] cmd_calc_nomina",{})
                if nd.get("_parse_error"):
                    raise ValueError(f"backend nomina devolvio respuesta no JSON ({nd.get('_http_status')})")
                s.respuesta=nd.get("resumen","No pude calcular") if nd.get("success") else nd.get("mensaje","Error")
            except Exception as e: s.respuesta=f"Error calculando nomina: {str(e)[:100]}"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # ENVIAR NOMINA: paso 1 → ask mes
        if esp.get("tipo")=="cmd_enviar_nom_1":
            sel=_sel(ctx.get("emps",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_enviar_nom_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            # Get employee phone for sending
            emp_tel=await db_get("empleados",f"id=eq.{sel['id']}&select=telefono")
            tel_dest=emp_tel[0].get("telefono","") if emp_tel else ""
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_enviar_nom_2","dominio":"CMD","contexto":{"emp_id":sel["id"],"emp_nombre":sel["nombre"],"tel_dest":tel_dest}})
            MESES_TXT="1=ENE 2=FEB 3=MAR 4=ABR 5=MAY 6=JUN 7=JUL 8=AGO 9=SEP 10=OCT 11=NOV 12=DIC"
            s.respuesta=f"Empleado: *{sel['nombre']}*\n\nQue mes? ({MESES_TXT})"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # ENVIAR NOMINA: paso 2 → send PDF
        if esp.get("tipo")=="cmd_enviar_nom_2":
            eid2=ctx.get("emp_id");enm=ctx.get("emp_nombre","?");tel_dest=ctx.get("tel_dest","")
            try: mes=int(s.mensaje_normalizado.strip())
            except: mes=datetime.now().month
            MESES_ABR={1:"ENE",2:"FEB",3:"MAR",4:"ABR",5:"MAY",6:"JUN",7:"JUL",8:"AGO",9:"SEP",10:"OCT",11:"NOV",12:"DIC"}
            mes_abr=MESES_ABR.get(mes,"");anio_short=str(datetime.now().year)[-2:]
            docs=await db_get("documentos",f"empleado_id=eq.{eid2}&tipo_documento=eq.NOMINA&mes=eq.{mes_abr}&anio=eq.{anio_short}&order=created_at.desc&limit=1&select=drive_file_id,nombre_archivo")
            if not docs:
                s.respuesta=f"No encontre nomina de {mes_abr}-{anio_short} para {enm}"
            else:
                doc=docs[0];ok=await enviar_doc_whatsapp(tel_dest or s.telefono,doc["drive_file_id"],doc.get("nombre_archivo","nomina.pdf"))
                s.respuesta=f"\u2705 Nomina {mes_abr}-{anio_short} enviada a *{enm}* \U0001f4c4" if ok else f"No pude enviar el PDF \U0001f527"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # ANTICIPO: paso 1 → ask mes
        if esp.get("tipo")=="cmd_anticipo_1":
            sel=_sel(ctx.get("emps",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_anticipo_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_anticipo_2","dominio":"CMD","contexto":{"emp_id":sel["id"],"emp_nombre":sel["nombre"]}})
            s.respuesta=f"Empleado: *{sel['nombre']}*\n\nQue mes? (1-12 o 0=todos)"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # ANTICIPO: paso 2 → result
        if esp.get("tipo")=="cmd_anticipo_2":
            eid2=ctx.get("emp_id");enm=ctx.get("emp_nombre","?")
            try: mes=int(s.mensaje_normalizado.strip())
            except: mes=0
            q=f"empleado_id=eq.{eid2}&select=importe,fecha,concepto&order=fecha.desc"
            if mes>0: q+=f"&mes=eq.{mes}"
            rows=await db_get("anticipos",q)
            if not rows: s.respuesta=f"*{enm}* no tiene anticipos"
            else:
                total=round(sum(float(r.get("importe",0) or 0) for r in rows),2)
                det="\n".join([f"  \U0001f4b3 {r.get('fecha','?')} — *{r.get('importe',0)}\u20ac* {r.get('concepto','')}" for r in rows[:20]])
                s.respuesta=f"\U0001f4b6 *Anticipos de {enm}:*\n\n{det}\n\n*TOTAL: {total}\u20ac* ({len(rows)})"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # REGISTRAR ANTICIPO: paso 1 → ask importe
        if esp.get("tipo")=="cmd_reg_ant_1":
            sel=_sel(ctx.get("emps",[]),s.mensaje_normalizado)
            if not sel:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_reg_ant_1","dominio":"CMD","contexto":ctx})
                s.respuesta="No encontre. Dime el numero.";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_reg_ant_2","dominio":"CMD","contexto":{"emp_id":sel["id"],"emp_nombre":sel["nombre"]}})
            s.respuesta=f"Empleado: *{sel['nombre']}*\n\nImporte del anticipo? (ej: 200)"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # REGISTRAR ANTICIPO: paso 2 → ask tipo
        if esp.get("tipo")=="cmd_reg_ant_2":
            try: importe=float(s.mensaje_normalizado.strip().replace(",",".").replace("€",""))
            except: importe=0
            if importe<=0:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_reg_ant_2","dominio":"CMD","contexto":ctx})
                s.respuesta="Importe no valido. Dime un numero (ej: 200)";s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
                if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                await guardar_ejecucion(s);return s
            ctx["importe"]=importe
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"cmd_reg_ant_3","dominio":"CMD","contexto":ctx})
            s.respuesta=f"Importe: *{importe}\u20ac*\n\nTipo?\n  1. Transferencia\n  2. Efectivo"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # REGISTRAR ANTICIPO: paso 3 → register
        if esp.get("tipo")=="cmd_reg_ant_3":
            eid2=ctx.get("emp_id");enm=ctx.get("emp_nombre","?");importe=ctx.get("importe",0)
            t2=s.mensaje_normalizado.strip().lower()
            tipo="transferencia" if t2 in("1","transferencia") else "efectivo"
            result=await db_post("anticipos",{"empleado_id":eid2,"empleado_nombre":enm,"importe":importe,"tipo":tipo,"fecha":date.today().isoformat(),"mes":datetime.now().month,"anio":datetime.now().year,"estado":"APROBADO","concepto":f"Anticipo {tipo}"})
            if isinstance(result,list) or (isinstance(result,dict) and "error" not in str(result).lower()[:50]):
                s.respuesta=f"\u2705 Anticipo registrado:\n\n\U0001f477 {enm}\n\U0001f4b3 {importe}\u20ac ({tipo})\n\U0001f4c5 {date.today().isoformat()}"
            else: s.respuesta=f"Error: {str(result)[:100]}"
            s.dominio="CMD";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
        
        # ═══ MENU EMPLEADO HANDLERS ═══
        # ADMIN MENU: number → execute command
        if esp.get("tipo") == "menu_admin":
            idioma = ctx.get("idioma", "es")
            try: sel = int(s.mensaje_normalizado.strip())
            except: sel = 0
            # Map admin numbers to CMD commands
            cmd_map_admin = {1:"ALTA OBRA",2:"CERRAR OBRA",3:"REABRIR OBRA",4:"ALTA EMPLEADO",5:"BAJA EMPLEADO",6:"HORAS OBRA",7:"GASTOS EMPLEADO",8:"GASTOS OBRA",9:"CALCULAR NOMINA",10:"ENVIAR NOMINA",11:"ANTICIPO",12:"REGISTRAR ANTICIPO"}
            emp_map_admin = {13:1,14:5,15:3,16:4,17:6,18:7}  # personal options → employee menu numbers
            if sel in cmd_map_admin:
                cmd_name = cmd_map_admin[sel]
                _cmd_map2 = {"HORAS OBRA":"CMD_HORAS_OBRA","GASTOS EMPLEADO":"CMD_GASTOS_EMP","GASTOS OBRA":"CMD_GASTOS_OBRA","CALCULAR NOMINA":"CMD_CALC_NOMINA","ENVIAR NOMINA":"CMD_ENVIAR_NOMINA","ANTICIPO":"CMD_ANTICIPO","REGISTRAR ANTICIPO":"CMD_REG_ANTICIPO","REABRIR OBRA":"REABRIR_OBRA","CERRAR OBRA":"CERRAR_OBRA","ALTA EMPLEADO":"ALTA_EMPLEADO","BAJA EMPLEADO":"BAJA_EMPLEADO","ALTA OBRA":"OBRA_ALTA"}
                s.dominio = _cmd_map2.get(cmd_name, "GENERAL"); s.dominio_fuente = "menu"
                try: s.respuesta = await AG.get(s.dominio, ag_general)(s)
                except Exception as e: s.respuesta = f"Error: {str(e)[:100]}"
            elif sel in emp_map_admin:
                # Redirect to employee menu handler
                ctx["from_admin"] = True
                s.mensaje_normalizado = str(emp_map_admin[sel])
                # Fall through to menu_emp handler below
                esp["tipo"] = "menu_emp"
            else:
                s.respuesta = "Dime un numero del 1 al 18"
                s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_admin", "dominio": "MENU", "contexto": ctx})
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
            if sel in cmd_map_admin:
                s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
        
        # ENCARGADO MENU: number → execute command or personal
        if esp.get("tipo") == "menu_enc":
            idioma = ctx.get("idioma", "es")
            try: sel = int(s.mensaje_normalizado.strip())
            except: sel = 0
            cmd_map_enc = {1:"ALTA OBRA",2:"CERRAR OBRA",3:"REABRIR OBRA",4:"ALTA EMPLEADO",5:"HORAS OBRA",6:"GASTOS EMPLEADO",7:"GASTOS OBRA"}
            emp_map_enc = {8:1,9:2,10:3,11:4,12:5,13:6,14:7,15:8}
            if sel in cmd_map_enc:
                cmd_name = cmd_map_enc[sel]
                _cmd_map2 = {"HORAS OBRA":"CMD_HORAS_OBRA","GASTOS EMPLEADO":"CMD_GASTOS_EMP","GASTOS OBRA":"CMD_GASTOS_OBRA","REABRIR OBRA":"REABRIR_OBRA","CERRAR OBRA":"CERRAR_OBRA","ALTA EMPLEADO":"ALTA_EMPLEADO","ALTA OBRA":"OBRA_ALTA"}
                s.dominio = _cmd_map2.get(cmd_name, "GENERAL"); s.dominio_fuente = "menu"
                try: s.respuesta = await AG.get(s.dominio, ag_general)(s)
                except Exception as e: s.respuesta = f"Error: {str(e)[:100]}"
                s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
            elif sel in emp_map_enc:
                ctx["from_enc"] = True
                s.mensaje_normalizado = str(emp_map_enc[sel])
                esp["tipo"] = "menu_emp"
            else:
                s.respuesta = "Dime un numero del 1 al 15" if idioma != "ro" else "Spune-mi un numar de la 1 la 15"
                s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_enc", "dominio": "MENU", "contexto": ctx})
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
        
        # EMPLOYEE MENU (also handles redirects from admin/encargado personal options)
        if esp.get("tipo") == "menu_emp":
            idioma = ctx.get("idioma", "es")
            try: sel = int(s.mensaje_normalizado.strip())
            except: sel = 0
            eid_m = s.empleado.get("id", 0)
            nombre_m = s.empleado.get("apodo") or s.empleado.get("nombre", "")
            
            def _meses_selector(idioma_m):
                """Generate last 12 months selector"""
                from datetime import date as d2
                hoy = d2.today()
                MNAMES_ES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
                MNAMES_RO = ["","Ianuarie","Februarie","Martie","Aprilie","Mai","Iunie","Iulie","August","Septembrie","Octombrie","Noiembrie","Decembrie"]
                mnames = MNAMES_RO if idioma_m == "ro" else MNAMES_ES
                meses = []
                for i in range(12):
                    m = hoy.month - i
                    y = hoy.year
                    if m <= 0: m += 12; y -= 1
                    meses.append({"num": m, "anio": y, "txt": f"{mnames[m]} {y}"})
                return meses
            
            if sel == 1:
                # FICHAR — list obras
                obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
                if not obras:
                    s.respuesta = "No hay obras activas" if idioma != "ro" else "Nu sunt lucrari active"
                else:
                    await guardar_contexto_resumido(s.telefono,eid_m,tema="fichaje",paso="elegir_obra")
                    lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
                    txt = f"\U0001f3d7 En que obra?\n\n{lista}\n\nDime numero" if idioma != "ro" else f"\U0001f3d7 La ce lucrare?\n\n{lista}\n\nSpune-mi numarul"
                    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid_m, "tipo": "menu_fichar_obra", "dominio": "MENU", "contexto": {"idioma": idioma, "obras": [{"id": o["id"], "nombre": o["nombre"]} for o in obras]}})
                    s.respuesta = txt
            elif sel in (2, 3, 4, 5, 6):
                # These all need month selector
                tema_map = {2: "nomina", 3: "cuanto_cobro", 4: "horas_trabajadas", 5: "enviar_nomina", 6: "anticipos"}
                await guardar_contexto_resumido(s.telefono,eid_m,tema=tema_map.get(sel,"consulta"),paso="elegir_mes")
                meses = _meses_selector(idioma)
                lista = "\n".join([f"  {i+1}. {m['txt']}" for i, m in enumerate(meses)])
                tipo_map = {2: "calc_nomina", 3: "cuanto_cobro", 4: "horas_trab", 5: "enviar_nomina", 6: "anticipos"}
                titulo_es = {2: "Calcular nomina", 3: "Cuanto cobro", 4: "Horas trabajadas", 5: "Enviar nomina", 6: "Mis anticipos"}
                titulo_ro = {2: "Calculeaza salariu", 3: "Cat castig", 4: "Ore lucrate", 5: "Trimite Nomina", 6: "Avansuri"}
                titulo = titulo_ro.get(sel, "") if idioma == "ro" else titulo_es.get(sel, "")
                txt_ask = "Ce luna?" if idioma == "ro" else "Que mes?"
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid_m, "tipo": f"menu_{tipo_map[sel]}", "dominio": "MENU", "contexto": {"idioma": idioma, "meses": meses}})
                s.respuesta = f"\U0001f4ca *{titulo}*\n\n{txt_ask}\n\n{lista}\n\nDime numero"
            elif sel == 7:
                # DATOS EMPRESA — fixed response
                if idioma == "ro":
                    s.respuesta = "\U0001f3e2 *Date firma:*\n\n*Obras y Servicios Euromir S.L.*\nCIF: B87075206\nAdresa: Plaza Enrique de Mesa 8\nCP: 28031\nTel: 911272723\nEmail: info@euromir.es"
                else:
                    s.respuesta = "\U0001f3e2 *Datos empresa:*\n\n*Obras y Servicios Euromir S.L.*\nCIF: B87075206\nDir: Plaza Enrique de Mesa 8\nCP: 28031\nTel: 911272723\nEmail: info@euromir.es"
            elif sel == 8:
                # MI ENCARGADO — list obras
                obras = await db_get("obras", "estado=eq.En curso&select=id,nombre&order=nombre")
                if not obras:
                    s.respuesta = "No hay obras activas"
                else:
                    await guardar_contexto_resumido(s.telefono,eid_m,tema="encargado",paso="elegir_obra")
                    lista = "\n".join([f"  {i+1}. *{o['nombre']}*" for i, o in enumerate(obras)])
                    txt = f"\U0001f477 En que obra?\n\n{lista}" if idioma != "ro" else f"\U0001f477 La ce lucrare?\n\n{lista}"
                    await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid_m, "tipo": "menu_encargado", "dominio": "MENU", "contexto": {"idioma": idioma, "obras": [{"id": o["id"], "nombre": o["nombre"]} for o in obras]}})
                    s.respuesta = txt
            else:
                s.respuesta = "Dime un numero del 1 al 8" if idioma != "ro" else "Spune-mi un numar de la 1 la 8"
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # MENU FICHAR: obra selected → ask hoy/ayer
        if esp.get("tipo") == "menu_fichar_obra":
            idioma = ctx.get("idioma", "es")
            sel_obra = None
            obras = ctx.get("obras", [])
            try:
                idx = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= idx < len(obras): sel_obra = obras[idx]
            except: pass
            if not sel_obra:
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_fichar_obra", "dominio": "MENU", "contexto": ctx})
                s.respuesta = "Numero no valido" if idioma != "ro" else "Numar invalid"
            else:
                await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="fichaje",obra=sel_obra["nombre"],paso="elegir_dia")
                # Check if ayer has fichaje
                from datetime import timedelta as td_m
                ayer = (date.today() - td_m(days=1)).isoformat()
                fich_ayer = await db_get("fichajes_tramos", f"empleado_id=eq.{s.empleado.get('id',0)}&fecha=eq.{ayer}&select=id&limit=1")
                opciones = "1. Hoy\n2. Ayer" if not fich_ayer else "1. Hoy"
                if idioma == "ro": opciones = "1. Azi\n2. Ieri" if not fich_ayer else "1. Azi"
                txt = f"Obra: *{sel_obra['nombre']}*\n\nPara que dia?\n{opciones}" if idioma != "ro" else f"Lucrare: *{sel_obra['nombre']}*\n\nPentru ce zi?\n{opciones}"
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_fichar_dia", "dominio": "MENU", "contexto": {"idioma": idioma, "obra": sel_obra, "tiene_ayer": not bool(fich_ayer)}})
                s.respuesta = txt
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # MENU FICHAR: dia selected → ask horas
        if esp.get("tipo") == "menu_fichar_dia":
            idioma = ctx.get("idioma", "es")
            t_sel = s.mensaje_normalizado.strip()
            fecha_fich = date.today().isoformat()
            if t_sel in ("2", "ayer", "ieri") and ctx.get("tiene_ayer"):
                from datetime import timedelta as td_m2
                fecha_fich = (date.today() - td_m2(days=1)).isoformat()
            await guardar_contexto_resumido(s.telefono,s.empleado.get("id",0),tema="fichaje",obra=(ctx.get("obra") or {}).get("nombre",""),paso="decir_horas",fecha=fecha_fich)
            txt = "Dime entrada y salida (ej: *9-18*):" if idioma != "ro" else "Spune-mi intrare si iesire (ex: *9-18*):"
            await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_fichar_horas", "dominio": "MENU", "contexto": {"idioma": idioma, "obra": ctx.get("obra"), "fecha": fecha_fich}})
            s.respuesta = txt
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # MENU FICHAR: horas entered → register via backend
        if esp.get("tipo") == "menu_fichar_horas":
            idioma = ctx.get("idioma", "es")
            obra = ctx.get("obra", {})
            fecha_fich = ctx.get("fecha", date.today().isoformat())
            # Parse hours and send to backend
            msg_norm = normalizar_horas(s.mensaje_normalizado)
            pf = parse_fichaje(msg_norm)
            if pf.get("ok") and pf.get("entrada") and pf.get("salida"):
                msg_backend = f"de {pf['entrada']} a {pf['salida']}"
                body = {"mensaje": msg_backend, "empleado_id": s.empleado.get("id", 0), "empleado_nombre": s.empleado.get("nombre", ""), "empleado_telefono": s.empleado.get("telefono", ""), "coste_hora": s.empleado.get("coste_hora", 0), "fuera_madrid_hora": 15, "fecha": fecha_fich}
                try:
                    async with httpx.AsyncClient(timeout=30) as c:
                        r = await c.post(f"{PYTHON_URL}/procesar-fichaje", json=body)
                        d = safe_json_response(r, f"[{s.trace_id}] menu_fichar_horas", {})
                    if d.get("_parse_error"):
                        raise ValueError(f"backend fichajes devolvio respuesta no JSON ({d.get('_http_status')})")
                    msg_r = d.get("mensaje", d.get("message", str(d)))
                    if "obra" in msg_r.lower() and "1." in msg_r:
                        # Backend asking for obra but we already know it — send selection
                        obra_ref = (obra.get("nombre") or "").strip()
                        if obra_ref:
                            async with httpx.AsyncClient(timeout=30) as c:
                                r2 = await c.post(f"{PYTHON_URL}/procesar-fichaje", json={**body, "mensaje": obra_ref})
                                d2 = safe_json_response(r2, f"[{s.trace_id}] menu_fichar_horas_obra", {})
                            if d2.get("_parse_error"):
                                raise ValueError(f"backend fichajes devolvio respuesta no JSON ({d2.get('_http_status')})")
                            msg_r = d2.get("mensaje", d2.get("message", str(d2)))
                    s.respuesta = msg_r
                except Exception as e:
                    s.respuesta = f"Error: {str(e)[:100]}"
            else:
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": "menu_fichar_horas", "dominio": "MENU", "contexto": ctx})
                s.respuesta = "No entendi. Ej: *9-18* o *8:30-17*" if idioma != "ro" else "Nu am inteles. Ex: *9-18* sau *8:30-17*"
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # MENU: month-based queries (calc_nomina, cuanto_cobro, horas_trab, enviar_nomina, anticipos)
        if esp.get("tipo") and esp["tipo"].startswith("menu_") and esp["tipo"] in ("menu_calc_nomina","menu_cuanto_cobro","menu_horas_trab","menu_enviar_nomina","menu_anticipos"):
            idioma = ctx.get("idioma", "es")
            meses = ctx.get("meses", [])
            try:
                idx = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= idx < len(meses): mes_sel = meses[idx]
                else: mes_sel = None
            except: mes_sel = None
            if not mes_sel:
                await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": s.empleado.get("id", 0), "tipo": esp["tipo"], "dominio": "MENU", "contexto": ctx})
                s.respuesta = "Dime el numero del mes" if idioma != "ro" else "Spune-mi numarul lunii"
                s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s); return s
            
            mes_n = mes_sel["num"]; anio_n = mes_sel["anio"]
            eid_m = s.empleado.get("id", 0); enm = s.empleado.get("nombre", "")
            tipo = esp["tipo"]
            tema_map = {"menu_calc_nomina":"nomina","menu_cuanto_cobro":"cuanto_cobro","menu_horas_trab":"horas_trabajadas","menu_enviar_nomina":"enviar_nomina","menu_anticipos":"anticipos"}
            await guardar_contexto_resumido(s.telefono,eid_m,tema=tema_map.get(tipo,"consulta"),empleado=enm,mes=mes_sel["txt"],paso="resultado")
            
            if tipo == "menu_calc_nomina" or tipo == "menu_cuanto_cobro":
                try:
                    async with httpx.AsyncClient(timeout=30) as c:
                        r = await c.post(f"{PYTHON_URL}/calcular-nomina", json={"empleado_nombre": enm, "mes": mes_n, "anio": anio_n})
                        d = safe_json_response(r, f"[{s.trace_id}] menu_nomina", {})
                    if d.get("_parse_error"):
                        raise ValueError(f"backend nomina devolvio respuesta no JSON ({d.get('_http_status')})")
                    s.respuesta = d.get("resumen", "No pude calcular") if d.get("success") else d.get("mensaje", "Error")
                except Exception as e: s.respuesta = f"Error: {str(e)[:100]}"
            
            elif tipo == "menu_horas_trab":
                f_inicio = f"{anio_n}-{mes_n:02d}-01"
                if mes_n == 12: f_fin = f"{anio_n+1}-01-01"
                else: f_fin = f"{anio_n}-{mes_n+1:02d}-01"
                rows = await db_get("fichajes_tramos", f"empleado_id=eq.{eid_m}&fecha=gte.{f_inicio}&fecha=lt.{f_fin}&select=horas_decimal,obra_nombre")
                total = round(sum(float(r.get("horas_decimal", 0) or 0) for r in rows), 1)
                obras_set = set((r.get("obra_nombre") or "?") for r in rows)
                txt_mes = mes_sel["txt"]
                if idioma == "ro":
                    s.respuesta = f"\U0001f552 *Ore lucrate {txt_mes}:* *{total}h* in {len(obras_set)} lucrari"
                else:
                    s.respuesta = f"\U0001f552 *Horas trabajadas {txt_mes}:* *{total}h* en {len(obras_set)} obras"
            
            elif tipo == "menu_enviar_nomina":
                MESES_ABR = {1:"ENE",2:"FEB",3:"MAR",4:"ABR",5:"MAY",6:"JUN",7:"JUL",8:"AGO",9:"SEP",10:"OCT",11:"NOV",12:"DIC"}
                mes_abr = MESES_ABR.get(mes_n, ""); anio_short = str(anio_n)[-2:]
                docs = await db_get("documentos", f"empleado_id=eq.{eid_m}&tipo_documento=eq.NOMINA&mes=eq.{mes_abr}&anio=eq.{anio_short}&order=created_at.desc&limit=1&select=drive_file_id,nombre_archivo")
                if not docs:
                    s.respuesta = f"No encontre nomina de {mes_abr}-{anio_short}" if idioma != "ro" else f"Nu am gasit salariul {mes_abr}-{anio_short}"
                else:
                    doc = docs[0]
                    # DNI verification for employees
                    _rol_m = int(s.empleado.get("rol_id", 99) or 99)
                    if _rol_m >= 3:
                        await db_post("bia_esperas", {"telefono": s.telefono, "empleado_id": eid_m, "tipo": "nomina_dni", "dominio": "NOMINA", "contexto": {"datos_nomina": {"empleado_nombre": enm, "mes": mes_n, "anio": anio_n}}})
                        s.respuesta = "Para enviarte la nomina necesito tu DNI/NIE:" if idioma != "ro" else "Pentru a-ti trimite salariul am nevoie de DNI/NIE:"
                    else:
                        ok = await enviar_doc_whatsapp(s.telefono, doc["drive_file_id"], doc.get("nombre_archivo", "nomina.pdf"))
                        s.respuesta = f"\u2705 Nomina {mes_abr}-{anio_short} enviada!" if ok else "No pude enviar"
            
            elif tipo == "menu_anticipos":
                rows = await db_get("anticipos", f"empleado_id=eq.{eid_m}&mes=eq.{mes_n}&anio=eq.{anio_n}&select=importe,fecha,concepto&order=fecha.desc")
                if not rows:
                    s.respuesta = f"No tienes anticipos en {mes_sel['txt']}" if idioma != "ro" else f"Nu ai avansuri in {mes_sel['txt']}"
                else:
                    total = round(sum(float(r.get("importe", 0) or 0) for r in rows), 2)
                    det = "\n".join([f"  \U0001f4b3 {r.get('fecha','?')} — *{r.get('importe',0)}\u20ac*" for r in rows])
                    s.respuesta = f"\U0001f4b6 *Anticipos {mes_sel['txt']}:*\n\n{det}\n\n*TOTAL: {total}\u20ac*"
            
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # MENU ENCARGADO: obra selected → show encargado
        if esp.get("tipo") == "menu_encargado":
            idioma = ctx.get("idioma", "es")
            obras = ctx.get("obras", [])
            sel_obra = None
            try:
                idx = int(s.mensaje_normalizado.strip()) - 1
                if 0 <= idx < len(obras): sel_obra = obras[idx]
            except: pass
            if not sel_obra:
                s.respuesta = "No encontre esa obra"
            else:
                obra_data = await db_get("obras", f"id=eq.{sel_obra['id']}&select=encargado_id")
                if obra_data and obra_data[0].get("encargado_id"):
                    enc = await db_get("empleados", f"id=eq.{obra_data[0]['encargado_id']}&select=nombre,telefono")
                    if enc:
                        enc_nombre = enc[0].get("nombre", "?"); enc_tel = enc[0].get("telefono", "?")
                        if idioma == "ro":
                            s.respuesta = f"\U0001f477 *Encargado de {sel_obra['nombre']}:*\n\n*{enc_nombre}*\nTel: {enc_tel}"
                        else:
                            s.respuesta = f"\U0001f477 *Encargado de {sel_obra['nombre']}:*\n\n*{enc_nombre}*\nTel: {enc_tel}"
                    else:
                        s.respuesta = "No encontre al encargado"
                else:
                    s.respuesta = "Esta obra no tiene encargado asignado"
            s.dominio = "MENU"; s.dominio_fuente = "espera"; s.duracion_ms = int((time.time() - t0) * 1000)
            if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
            await guardar_ejecucion(s); return s
        
        # Handle confirmar_fichaje espera — user confirming LLM-parsed hours
        if esp.get("tipo")=="confirmar_fichaje" and consume_espera:
            resp_low=s.mensaje_normalizado.lower().strip()
            if resp_low in("si","sí","ok","vale","correcto","yes"):
                # User confirmed — rebuild message from saved hours and process as fichaje
                ent_conf=ctx.get("entrada","");sal_conf=ctx.get("salida","")
                if ent_conf and sal_conf:
                    s.mensaje_normalizado=f"de {ent_conf} a {sal_conf}";s.mensaje_original=s.mensaje_normalizado
                    s.dominio="FICHAJE";s.dominio_fuente="espera";s.confianza=1.0;s.accion="procesar"
                    s.timer_start("agente")
                    try:s.respuesta=await AG.get("FICHAJE",ag_general)(s)
                    except Exception as e:s.add_error(f"Confirm: {e}");s.respuesta="Problema con el fichaje \U0001f527"
                    s.timer_end("agente");s.duracion_ms=int((time.time()-t0)*1000)
                    if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
                    await guardar_ejecucion(s);return s
            else:
                # User wants to repeat hours — let it flow through normal detection
                log.info(f"[{s.trace_id}] Nuevas horas tras confirmar, continuando sin reutilizar espera")
                consume_espera=False
        if consume_espera:
            s.dominio=esp.get("dominio","FICHAJE");s.dominio_fuente="espera";s.confianza=1.0;s.accion="continuar";s.metadata["espera_contexto"]=ctx
            s.timer_start("agente")
            try:s.respuesta=await AG.get(s.dominio,ag_general)(s)
            except Exception as e:s.add_error(f"Espera: {e}");s.respuesta="Problema tecnico \U0001f527"
            s.timer_end("agente");s.duracion_ms=int((time.time()-t0)*1000)
            if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
            await guardar_ejecucion(s);return s
    # ═══ CHECK INTENTS FIRST (deterministic, no AI) ═══
    rol_intent = rol_actual
    intent = detectar_intent(s.mensaje_normalizado, rol_intent)
    if intent:
        s.timer_start("intent")
        s.dominio = "INTENT"
        s.dominio_fuente = "intent"
        s.accion = intent["id"]
        log.info(f"[{s.trace_id}] \U0001f4a1 Intent: {intent['id']} ({intent['tipo']})")
        try:
            resp = await ejecutar_intent(s, intent)
            if resp:
                s.respuesta = resp
                s.timer_end("intent")
                s.duracion_ms = int((time.time() - t0) * 1000)
                log.info(f"[{s.trace_id}] \u2705 Intent {intent['id']} {s.duracion_ms}ms")
                if s.respuesta: await guardar_msg(s.telefono, s.empleado.get("id", 0), "assistant", s.respuesta)
                await guardar_ejecucion(s)
                return s
        except Exception as e:
            log.error(f"[{s.trace_id}] Intent error: {e}")
            s.add_error(f"Intent: {e}")
        s.timer_end("intent")
    
    ctx_activo=await cargar_contexto_resumido(s.telefono)
    # Detector (with confirmation protection)
    # Re-check esperas for confirmation context (previous espera was consumed, check if new one exists)
    conf_esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&dominio=eq.FICHAJE&order=created_at.desc&limit=1&select=tipo,dominio")
    tiene_espera_conf=bool(conf_esperas and conf_esperas[0].get("tipo") in ("seleccion_obra","confirmar_fichaje","confirmar_turno"))
    s.timer_start("detector")
    dom,acc,conf=detectar(s.mensaje_normalizado,rol_actual,tiene_espera_conf)
    s.timer_end("detector")
    if dom!="AMBIGUO":
        s.dominio,s.accion,s.confianza,s.dominio_fuente=dom,acc,conf,"regex"
        log.info(f"[{s.trace_id}] \U0001f3af Regex: {dom}")
    else:
        s.timer_start("clasificador");dom,conf=await clasificar(s.mensaje_normalizado);s.timer_end("clasificador")
        fuente_dom="gpt"
        if dom in ("GENERAL","OBRAS","FINANZAS") and es_followup_empleado_ctx(s.mensaje_normalizado,ctx_activo):
            dom,conf="EMPLEADOS",0.93
            fuente_dom="followup_empleado"
            log.info(f"[{s.trace_id}] \U0001f477 Follow-up empleado: {s.mensaje_normalizado}")
        s.dominio,s.confianza,s.dominio_fuente=dom,conf,fuente_dom
        log.info(f"[{s.trace_id}] \U0001f916 GPT: {dom} ({conf})")
    if not s.empleado.get("id"):s.add_error("Sin empleado",False);s.respuesta="No te identifique.";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
    s.timer_start("agente")
    try:
        result=await AG.get(s.dominio,ag_general)(s)
        if result is None and s.dominio=="FICHAJE":
            # ag_fichaje returned None — LLM said not a fichaje, route to general
            log.info(f"[{s.trace_id}] Fichaje returned None, routing to general")
            s.dominio="GENERAL";s.dominio_fuente="fichaje_fallback"
            result=await ag_general(s)
        s.respuesta=result or ""
    except Exception as e:s.add_error(f"Agente: {e}");s.respuesta="Problema tecnico \U0001f527"
    s.timer_end("agente");s.duracion_ms=int((time.time()-t0)*1000)
    log.info(f"[{s.trace_id}] \u2705 {s.dominio} {s.duracion_ms}ms")
    if s.respuesta:await guardar_msg(s.telefono,s.empleado.get("id",0),"assistant",s.respuesta)
    if s.respuesta and not s.errores:
        await registrar_memoria_turno(s)
    await guardar_ejecucion(s);return s

# ══════════════ WEBHOOK ══════════════
@app.post("/webhook")
async def webhook(req:Request):
    try:
        data=await req.json();event=data.get("event","")
        if event not in("messages.upsert",""):return{"ok":True}
        msg=data.get("data",{});msg_key=msg.get("key",{})
        remote=msg_key.get("remoteJid","");fm=msg_key.get("fromMe",False)
        if fm:return{"ok":True}
        tel=remote.replace("@s.whatsapp.net","")
        m=msg.get("message",{})
        cont=m.get("conversation","") or m.get("extendedTextMessage",{}).get("text","")
        has_image=bool(m.get("imageMessage"))
        has_audio=bool(m.get("audioMessage"))
        has_doc=bool(m.get("documentMessage"))
        # Image handler
        if has_image:
            log.info(f"Image from {tel}")
            emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
            if not emps:await wa(f"{tel}@s.whatsapp.net","No estas registrado.");return{"ok":True}
            emp=emps[0];img_msg=m.get("imageMessage",{})
            try:
                import base64 as b64mod
                img_b64=""
                async with httpx.AsyncClient(timeout=30) as dl:
                    evo_r=await dl.post(f"{EVO}/chat/getBase64FromMediaMessage/{INSTANCE}",headers={"apikey":EK,"Content-Type":"application/json"},json={"message":{"key":msg_key},"convertToMp4":False})
                    if evo_r.status_code in(200,201):
                        evo_d=safe_json_response(evo_r,"image_base64",{})
                        img_b64=evo_d.get("base64","")
                        log.info(f"Evo base64: {len(img_b64)} chars")
                if img_b64:
                    mime=img_msg.get("mimetype","image/jpeg")
                    ocr_msgs=[{"role":"user","content":[{"type":"text","text":"Analiza esta factura/ticket. Extrae: proveedor, CIF, fecha (YYYY-MM-DD), concepto, base_imponible, iva_porcentaje, iva_importe, total. SOLO JSON sin markdown."},{"type":"image_url","image_url":{"url":f"data:{mime};base64,{img_b64}"}}]}]
                    async with httpx.AsyncClient(timeout=60) as oc:
                        ocr_r=await oc.post("https://api.openai.com/v1/chat/completions",headers={"Authorization":f"Bearer {OPENAI_KEY}","Content-Type":"application/json"},json={"model":"gpt-4o-mini","messages":ocr_msgs,"max_tokens":500})
                        ocr_data=safe_json_response(ocr_r,"ocr_factura",{"error":"invalid json"})
                        if "error" in ocr_data:ocr_raw='{"proveedor":"No pude leer","total":0}'
                        else:ocr_raw=ocr_data["choices"][0]["message"]["content"]
                    if "```" in ocr_raw:ocr_raw=ocr_raw.split("```")[1].replace("json","").strip()
                    try:factura_data=json.loads(ocr_raw)
                    except:factura_data={"proveedor":"?","total":0}
                    drive_url=""
                    try:
                        import io as iomod
                        img_bytes=b64mod.b64decode(img_b64)
                        files_got={"files":("factura.jpg",iomod.BytesIO(img_bytes),"image/jpeg")}
                        async with httpx.AsyncClient(timeout=30) as gotc:
                            got_r=await gotc.post(f"{GOTENBERG}/forms/libreoffice/convert",files=files_got)
                            if got_r.status_code==200:
                                pdf_b64=b64mod.b64encode(got_r.content).decode()
                                prov_name=factura_data.get("proveedor","fac")[:30].replace(" ","_")
                                fecha_name=factura_data.get("fecha","")[:10]
                                try:
                                    fdate=factura_data.get("fecha","");month=int(fdate[5:7]) if fdate and len(fdate)>=7 else 1
                                    trim_key=f"T{(month-1)//3+1}"
                                except:trim_key="T1"
                                folder_id=DRIVE_FOLDERS.get(trim_key,DRIVE_FOLDERS.get("T1",""))
                                async with httpx.AsyncClient(timeout=30) as drc:
                                    dr_r=await drc.post(f"{N8N}/webhook/upload-factura-drive",json={"pdf_base64":pdf_b64,"filename":f"FAC_{prov_name}_{fecha_name}.pdf","folder_id":folder_id})
                                    if dr_r.status_code==200:drive_url=safe_json_response(dr_r,"upload_factura_drive",{}).get("url","")
                    except Exception as e:log.error(f"PDF/Drive: {e}")
                    obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
                    obras_txt="\n".join([f"{i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
                    prov=factura_data.get("proveedor","?");total=factura_data.get("total",0);fecha=factura_data.get("fecha","?")
                    resp=f"He leido esta factura:\n\nProveedor: {prov}\nFecha: {fecha}\nTotal: {total}EUR\n\nA que obra va? \U0001f3d7\n\n{obras_txt}\n\nDime numero o nombre \U0001f60a"
                    await wa(f"{tel}@s.whatsapp.net",resp)
                    await db_post("bia_esperas",{"telefono":tel,"empleado_id":emp.get("id",0),"tipo":"factura_obra","dominio":"FACTURA","contexto":{"factura":factura_data,"obras":[o["id"] for o in obras],"drive_url":drive_url}})
                    await db_post("bia_ejecuciones",{"trace_id":str(uuid.uuid4())[:8],"telefono":tel,"empleado_id":emp.get("id"),"empleado_nombre":emp.get("nombre",""),"input_original":"[imagen]","dominio":"FACTURA","dominio_fuente":"media","estado_final":"ok","respuesta":resp[:500],"duracion_ms":0})
                else:await wa(f"{tel}@s.whatsapp.net","No pude descargar la imagen \U0001f527")
            except Exception as e:log.error(f"Img: {e}");await wa(f"{tel}@s.whatsapp.net","No pude leer la factura \U0001f527")
            return{"ok":True}
        # Audio handler
        if has_audio:
            log.info(f"Audio from {tel}")
            emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
            if not emps:await wa(f"{tel}@s.whatsapp.net","No estas registrado.");return{"ok":True}
            emp=emps[0]
            try:
                import base64 as b64mod,io as iomod
                async with httpx.AsyncClient(timeout=30) as dl:
                    evo_r=await dl.post(f"{EVO}/chat/getBase64FromMediaMessage/{INSTANCE}",headers={"apikey":EK,"Content-Type":"application/json"},json={"message":{"key":msg_key},"convertToMp4":False})
                    if evo_r.status_code in(200,201):audio_b64=safe_json_response(evo_r,"audio_base64",{}).get("base64","")
                    else:audio_b64=""
                if audio_b64:
                    audio_bytes=b64mod.b64decode(audio_b64)
                    files={"file":("audio.ogg",iomod.BytesIO(audio_bytes),"audio/ogg")}
                    async with httpx.AsyncClient(timeout=30) as wc:
                        wr=await wc.post("https://api.openai.com/v1/audio/transcriptions",headers={"Authorization":f"Bearer {OPENAI_KEY}"},data={"model":"whisper-1"},files=files)
                        transcription=safe_json_response(wr,"audio_transcription",{}).get("text","")
                    if transcription:
                        await guardar_msg(tel,emp.get("id",0),"user",f"[audio] {transcription}")
                        s=BiaState(telefono=tel,mensaje_original=transcription,tipo_mensaje="audio",empleado=emp);s=await procesar(s)
                        if s.respuesta:
                            await wa(f"{tel}@s.whatsapp.net",s.respuesta)
                        return{"ok":True,"trace_id":s.trace_id}
                    else:await wa(f"{tel}@s.whatsapp.net","No pude entender el audio \U0001f3a4")
                else:await wa(f"{tel}@s.whatsapp.net","No pude descargar el audio \U0001f527")
            except Exception as e:log.error(f"Audio: {e}");await wa(f"{tel}@s.whatsapp.net","Problema con el audio \U0001f3a4")
            return{"ok":True}
        if has_doc:
            try:
                async with httpx.AsyncClient(timeout=30) as fc:await fc.post(N8N_WEBHOOK or f"{N8N}/webhook/whatsapp-euromir",json=data)
            except:pass
            return{"ok":True}
        if not cont or not tel:return{"ok":True}
        # Dedup (skip for pure numbers — they're always menu selections)
        is_selection=cont.strip().isdigit() and 1<=int(cont.strip())<=20
        if not is_selection:
            last=await db_get("bia_chat_history",f"telefono=eq.{tel}&role=eq.user&order=created_at.desc&limit=1&select=content")
            if last and last[0].get("content","")==cont:log.info(f"Dup from {tel}");return{"ok":True}
        emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
        if not emps:await wa(f"{tel}@s.whatsapp.net","No estas registrado.");return{"ok":True}
        emp=emps[0];await guardar_msg(tel,emp.get("id",0),"user",cont)
        s=BiaState(telefono=tel,mensaje_original=cont,tipo_mensaje="texto",empleado=emp);s=await procesar(s)
        if s.respuesta:
            await wa(f"{tel}@s.whatsapp.net",s.respuesta)
        return{"ok":True,"trace_id":s.trace_id}
    except Exception as e:log.error(f"Webhook: {e}");return{"ok":False}

@app.post("/test")
async def test(req:Request):
    d=await req.json();emp=d.get("empleado",{"id":1,"nombre":"Test","telefono":"0","apodo":"Test","rol":1,"coste_hora":20})
    s=BiaState(telefono=emp.get("telefono","0"),mensaje_original=d.get("mensaje","hola"),empleado=emp);s=await procesar(s)
    return{"trace_id":s.trace_id,"dominio":s.dominio,"dominio_fuente":s.dominio_fuente,"confianza":s.confianza,"respuesta":s.respuesta,"errores":s.errores,"duracion_ms":s.duracion_ms}

@app.get("/health")
async def health():return{"status":"ok","service":"bia-v3","version":"7.6.4"}

if __name__=="__main__":
    import uvicorn;uvicorn.run(app,host="0.0.0.0",port=PORT)
