#!/usr/bin/env python3
"""BIA v7.0 — Fichajes Blindados + Parser v2 + Personalidad + Memoria + Facturas"""
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
            r=await c.get(f"{SUPA}/rest/v1/{t}?{q}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"}); return r.json() if r.status_code==200 else []
    except: return []

async def db_post(t,d):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r=await c.post(f"{SUPA}/rest/v1/{t}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json=d)
            return r.json() if r.status_code in(200,201) else {"error":r.text}
    except Exception as e: return {"error":str(e)}

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
            return r.json()["choices"][0]["message"]["content"]
    except Exception as e: log.error(f"GPT: {e}"); return ""

async def cargar_historial(tel,limit=20):
    msgs=await db_get("bia_chat_history",f"telefono=eq.{tel}&order=created_at.desc&limit={limit}&select=role,content")
    return list(reversed(msgs))

async def guardar_msg(tel,eid,role,content):
    await db_post("bia_chat_history",{"telefono":tel,"empleado_id":eid,"role":role,"content":(content or "")[:1000]})

# ══════════════ PERSONALIDAD ══════════════
BIA_PERSONA="Eres Bia, secretaria inteligente de Euromir. Cercana, directa, con chispa y humor. Emojis con naturalidad. CORTO para WhatsApp (3-5 lineas). Adapta el tono segun las notas del empleado."

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
        if 5<=sh<=12 and 1<=eh<=9 and gap>=3:
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
    if FK.search(t) or FK2.search(t): return {"det":True,"pat":"kw","needs_llm":True}
    return {"det":False}

def normalizar_horas(texto):
    t=texto
    def tarde_fix(m):
        h=int(m.group(1)); return str(h+12 if h<12 else h)
    t=re.sub(r'(\d{1,2})\s*(?:de la tarde|de la noche|pm|p\.m\.)',tarde_fix,t,flags=re.I)
    t=re.sub(r'(\d{1,2})\s*(?:de la ma.ana|am|a\.m\.)',r'\1',t,flags=re.I)
    return t

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

# ══════════════ DETECTOR ══════════════
PS=re.compile(r'^(hola|buenos d[ií]as|buenas( tardes| noches)?|qu[eé] tal|hey)[\s!.?]*$',re.I)
PC=re.compile(r'\b(confirmo|todos ok|confirmado|ok equipo|vale[,.]?\s*confirmado)\b',re.I)

def detectar(txt,empleado_rol=0,tiene_espera_conf=False):
    t=txt.lower().strip()
    pf=parse_fichaje(t)
    if pf.get("det"): return "FICHAJE","procesar",1.0
    # Protected confirmation
    if PC.search(t):
        if empleado_rol in(1,2) or tiene_espera_conf:
            return "FICHAJE","confirmar",0.9
        # Not valid confirmation context — fall through to general
    if PS.match(t): return "SALUDO","responder",1.0
    if re.search(r'nueva obra|registra(r|me)?\s*obra|abrir obra|alta obra|dar de alta obra',t): return "OBRA_ALTA","crear",0.95
    if re.search(r'cerrar obra|baja obra|dar de baja',t): return "OBRA_BAJA","cerrar",0.9
    return "AMBIGUO","clasificar",0.0

# ══════════════ CLASIFICADOR GPT ══════════════
async def clasificar(txt):
    p=f'Clasifica en UN dominio y da confianza. Dominios: FICHAJE,OBRAS,FINANZAS,EMPLEADOS,DOCUMENTOS,INVENTARIO,GENERAL. JSON: {{"dominio":"OBRAS","confianza":0.85}}. Mensaje: "{txt[:500]}"'
    raw=await gpt(p)
    try:
        if "```" in raw: raw=raw.split("```")[1].replace("json","").strip()
        d=json.loads(raw.strip());dom=d.get("dominio","GENERAL").upper();conf=float(d.get("confianza",0.5))
        return (dom if dom in ["FICHAJE","OBRAS","FINANZAS","EMPLEADOS","DOCUMENTOS","INVENTARIO","GENERAL"] else "GENERAL"),conf
    except: return "GENERAL",0.3

# ══════════════ AGENTE FICHAJE BLINDADO ══════════════
async def ag_fichaje(s):
    """Complete armored fichaje flow: regex → LLM → normalize → validate → idempotency → register → log"""
    s.timer_start("fichaje")
    t0=time.time()
    texto=s.mensaje_normalizado
    emp_id=s.empleado.get("id",0)
    hoy=date.today().isoformat()
    
    # Continuation from espera (obra selection) — forward directly to backend without re-parsing
    if s.accion=="continuar" and s.dominio_fuente=="espera":
        try:
            body={"mensaje":texto,"empleado_id":emp_id,"empleado_nombre":s.empleado.get("nombre",""),
                  "empleado_telefono":s.empleado.get("telefono",""),"coste_hora":s.empleado.get("coste_hora",0),"fuera_madrid_hora":15}
            async with httpx.AsyncClient(timeout=30) as c:
                r=await c.post(f"{PYTHON_URL}/procesar-fichaje",json=body)
                d=r.json()
            msg=d.get("mensaje",d.get("message",str(d)))
            if d.get("error") or "error" in str(d).lower()[:50]:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
            if "obra" in msg.lower() and "1." in msg:
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"ok":True}})
            s.timer_end("fichaje"); return msg
        except Exception as e:
            s.add_error(f"Fichaje espera: {e}"); s.timer_end("fichaje")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
            return "Problema con el fichaje. Repite el numero de obra \U0001f527"
    
    # Confirmation flow — pass through to backend
    if s.accion=="confirmar":
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                r=await c.post(f"{PYTHON_URL}/confirmar-fichajes",json={"respuesta":texto,"fecha":""})
                msg=r.json().get("mensaje",r.json().get("message",str(r.json())))
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
    body={"mensaje":texto,"empleado_id":emp_id,"empleado_nombre":s.empleado.get("nombre",""),
          "empleado_telefono":s.empleado.get("telefono",""),"coste_hora":s.empleado.get("coste_hora",0),
          "fuera_madrid_hora":15}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(f"{PYTHON_URL}/procesar-fichaje",json=body)
            d=r.json()
        msg=d.get("mensaje",d.get("message",str(d)))
        resultado="registrado"
        
        if d.get("error") or "error" in str(d).lower()[:50]:
            resultado="error"
            if s.dominio_fuente=="espera":
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"retry":True}})
        
        if "obra" in msg.lower() and "1." in msg:
            log.info(f"[{s.trace_id}] Saving espera seleccion_obra")
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":emp_id,"tipo":"seleccion_obra","dominio":"FICHAJE","contexto":{"ok":True}})
        
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
        return "Problema con el fichaje. Repite el numero de obra \U0001f527"

# ══════════════ OTROS AGENTES ══════════════
async def ag_saludo(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre","compañero")
    nt2=s.empleado.get("notas_bia","")
    return await gpt(f'El empleado {n} te saluda: "{s.mensaje_normalizado}". Responde un saludo corto y cercano.',BIA_PERSONA+f" Hablas con: {n}. Notas: {nt2}. Max 2 lineas.","gpt-4o-mini",150) or f"¡Buenas, {n}! Dime \U0001f4aa"

async def ag_obras(s):
    s.timer_start("obras");obras=await db_get("obras","select=id,nombre,estado,direccion,presupuesto_total,spreadsheet_id&estado=eq.En curso&order=nombre")
    n=s.empleado.get("apodo") or s.empleado.get("nombre","")
    r2=await gpt(f'Obras en curso:\n{json.dumps(obras,ensure_ascii=False)[:2000]}\n\nPregunta: "{s.mensaje_normalizado}"\nCorto para WhatsApp.',BIA_PERSONA+f" Hablas con {n}.","gpt-4o")
    s.timer_end("obras"); return r2 or "No pude consultar obras. \U0001f527"

async def ag_finanzas(s):
    s.timer_start("finanzas");g2=await db_get("gastos","select=id,concepto,total,obra,proveedor&order=created_at.desc&limit=10")
    r2=await gpt(f'Gastos recientes:\n{json.dumps(g2,ensure_ascii=False)[:1500]}\n\nPregunta: "{s.mensaje_normalizado}"',"Eres Bia. Corto.","gpt-4o")
    s.timer_end("finanzas"); return r2 or "No pude consultar finanzas. \U0001f527"

async def ag_empleados(s):
    s.timer_start("emps");e2=await db_get("empleados","select=id,nombre,cargo,estado&estado=eq.activo&order=nombre")
    r2=await gpt(f'Empleados:\n{json.dumps(e2,ensure_ascii=False)[:1500]}\n\nPregunta: "{s.mensaje_normalizado}"',"Eres Bia. Corto.","gpt-4o")
    s.timer_end("emps"); return r2 or "No pude consultar empleados. \U0001f527"

async def ag_general(s):
    n=s.empleado.get("apodo") or s.empleado.get("nombre","")
    nt2=s.empleado.get("notas_bia","")
    hist=await cargar_historial(s.telefono)
    h="".join([("Emp: " if m.get("role")=="user" else "Bia: ")+m.get("content","")+"\n" for m in hist[-8:]]) if hist else ""
    return await gpt(s.mensaje_normalizado,BIA_PERSONA+f"\nHablas con: {n}\nNotas: {nt2}\nHistorial:\n{h}","gpt-4o") or f"Perdona {n}, no te entendi \U0001f914"

async def ag_obra_alta(s):
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_nombre","dominio":"OBRA_ALTA","contexto":{}})
    return "Vamos a abrir una obra! \U0001f3d7\ufe0f Como se llama?"

async def ag_obra_baja(s):
    obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
    lista="\n".join([f"{i+1}. *{o['nombre']}*" for i,o in enumerate(obras)])
    await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_baja","dominio":"OBRA_BAJA","contexto":{"obras_ids":[o["id"] for o in obras]}})
    return f"Que obra quieres cerrar?\n\n{lista}\n\nDime numero o nombre"

AG={"FICHAJE":ag_fichaje,"OBRA_ALTA":ag_obra_alta,"OBRA_BAJA":ag_obra_baja,"SALUDO":ag_saludo,"OBRAS":ag_obras,"FINANZAS":ag_finanzas,"EMPLEADOS":ag_empleados,"DOCUMENTOS":ag_general,"INVENTARIO":ag_general,"GENERAL":ag_general}

# ══════════════ ROUTER PRINCIPAL ══════════════
async def procesar(s):
    t0=time.time();s.trace_id=str(uuid.uuid4())[:8]
    s.mensaje_normalizado=normalizar_horas(s.mensaje_original.strip())
    log.info(f"[{s.trace_id}] \U0001f4e9 {s.empleado.get('nombre','?')}: {s.mensaje_original[:80]}")
    # Espera activa
    esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&order=created_at.desc&limit=1")
    if esperas:
        esp=esperas[0];log.info(f"[{s.trace_id}] \u23f3 Espera: {esp['tipo']}")
        try:
            async with httpx.AsyncClient(timeout=10) as c:await c.delete(f"{SUPA}/rest/v1/bia_esperas?id=eq.{esp['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}"})
        except:pass
        ctx=esp.get("contexto",{}) or {}
        # Factura obra selection
        if esp.get("tipo")=="factura_obra":
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
            await guardar_ejecucion(s);return s
        # Obra alta steps
        if esp.get("tipo")=="obra_nombre":
            ctx["nombre"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_direccion","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="Direccion de la obra?";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_direccion":
            ctx["direccion"]=s.mensaje_normalizado
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_madrid","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta="Dentro o fuera de Madrid?";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_madrid":
            ctx["fuera_madrid"]="fuera" in s.mensaje_normalizado.lower()
            encs=await db_get("empleados","select=id,nombre,rol&rol=in.(1,2)&estado=eq.activo&order=nombre")
            lista="\n".join([f"{i+1}. {e2['nombre']}" + (" (Admin)" if e2['rol']==1 else "") for i,e2 in enumerate(encs)])
            ctx["encargados"]=[e2["id"] for e2 in encs]
            await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":s.empleado.get("id",0),"tipo":"obra_encargado","dominio":"OBRA_ALTA","contexto":ctx})
            s.respuesta=f"Quien es el encargado?\n\n{lista}\n\nDime numero o nombre";s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_encargado":
            encs=await db_get("empleados","select=id,nombre,rol&rol=in.(1,2)&estado=eq.activo&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1;enc=encs[sel] if 0<=sel<len(encs) else encs[0]
            except:enc=next((e2 for e2 in encs if s.mensaje_normalizado.lower() in e2["nombre"].lower()),encs[0])
            try:
                obra_data={"nombre":ctx.get("nombre",""),"direccion":ctx.get("direccion",""),"tipo":"reforma","presupuesto":0,"telefono":s.empleado.get("telefono",""),"encargado_id":enc["id"],"encargado_nombre":enc["nombre"],"fuera_madrid":ctx.get("fuera_madrid",False)}
                async with httpx.AsyncClient(timeout=60) as oc:await oc.post(f"{N8N}/webhook/alta-obra",json=obra_data)
                nombre_o=ctx.get("nombre","");dir_o=ctx.get("direccion","");fm="Fuera de Madrid" if ctx.get("fuera_madrid") else "Madrid"
                s.respuesta=f"\u2705 Obra creada!\n\n\U0001f3d7 {nombre_o}\n\U0001f4cd {dir_o}\n\U0001f30d {fm}\n\U0001f477 Encargado: {enc['nombre']}\n\n\U0001f4c1 Carpeta Drive + Sheet creados"
            except Exception as e:log.error(f"WF-15: {e}");s.respuesta="Error creando la obra."
            s.dominio="OBRA_ALTA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        if esp.get("tipo")=="obra_baja":
            obras=await db_get("obras","select=id,nombre,spreadsheet_id&estado=eq.En curso&order=nombre")
            try:
                sel=int(s.mensaje_normalizado.strip())-1;obra=obras[sel] if 0<=sel<len(obras) else None
            except:obra=None
            if obra:
                async with httpx.AsyncClient(timeout=15) as pc:await pc.patch(f"{SUPA}/rest/v1/obras?id=eq.{obra['id']}",headers={"apikey":SK,"Authorization":f"Bearer {SK}","Content-Type":"application/json","Prefer":"return=representation"},json={"estado":"Cerrada"})
                s.respuesta=f"\u2705 Obra *{obra['nombre']}* cerrada."
            else:s.respuesta="No encontre esa obra."
            s.dominio="OBRA_BAJA";s.dominio_fuente="espera";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        if esp.get("tipo")=="n8n_pending":
            try:
                fwd={"data":{"key":{"remoteJid":f"{s.telefono}@s.whatsapp.net","fromMe":False},"message":{"conversation":s.mensaje_original}}}
                async with httpx.AsyncClient(timeout=30) as fc:await fc.post(N8N_WEBHOOK or f"{N8N}/webhook/whatsapp-euromir",json=fwd)
                await db_post("bia_esperas",{"telefono":s.telefono,"empleado_id":0,"tipo":"n8n_pending","dominio":"N8N","contexto":{"text":True}})
            except:pass
            s.dominio="N8N";s.dominio_fuente="espera";s.respuesta="";s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
        # General espera
        s.dominio=esp.get("dominio","FICHAJE");s.dominio_fuente="espera";s.confianza=1.0;s.accion="continuar"
        s.timer_start("agente")
        try:s.respuesta=await AG.get(s.dominio,ag_general)(s)
        except Exception as e:s.add_error(f"Espera: {e}");s.respuesta="Problema tecnico \U0001f527"
        s.timer_end("agente");s.duracion_ms=int((time.time()-t0)*1000);await guardar_ejecucion(s);return s
    # Detector (with confirmation protection)
    # Re-check esperas for confirmation context (previous espera was consumed, check if new one exists)
    conf_esperas=await db_get("bia_esperas",f"telefono=eq.{s.telefono}&dominio=eq.FICHAJE&order=created_at.desc&limit=1&select=tipo,dominio")
    tiene_espera_conf=bool(conf_esperas and conf_esperas[0].get("tipo") in ("seleccion_obra","confirmar_fichaje","confirmar_turno"))
    s.timer_start("detector")
    dom,acc,conf=detectar(s.mensaje_normalizado,s.empleado.get("rol",0),tiene_espera_conf)
    s.timer_end("detector")
    if dom!="AMBIGUO":
        s.dominio,s.accion,s.confianza,s.dominio_fuente=dom,acc,conf,"regex"
        log.info(f"[{s.trace_id}] \U0001f3af Regex: {dom}")
    else:
        s.timer_start("clasificador");dom,conf=await clasificar(s.mensaje_normalizado);s.timer_end("clasificador")
        s.dominio,s.confianza,s.dominio_fuente=dom,conf,"gpt"
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
                    if evo_r.status_code in(200,201):img_b64=evo_r.json().get("base64","");log.info(f"Evo base64: {len(img_b64)} chars")
                if img_b64:
                    mime=img_msg.get("mimetype","image/jpeg")
                    ocr_msgs=[{"role":"user","content":[{"type":"text","text":"Analiza esta factura/ticket. Extrae: proveedor, CIF, fecha (YYYY-MM-DD), concepto, base_imponible, iva_porcentaje, iva_importe, total. SOLO JSON sin markdown."},{"type":"image_url","image_url":{"url":f"data:{mime};base64,{img_b64}"}}]}]
                    async with httpx.AsyncClient(timeout=60) as oc:
                        ocr_r=await oc.post("https://api.openai.com/v1/chat/completions",headers={"Authorization":f"Bearer {OPENAI_KEY}","Content-Type":"application/json"},json={"model":"gpt-4o-mini","messages":ocr_msgs,"max_tokens":500})
                        ocr_data=ocr_r.json()
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
                                    if dr_r.status_code==200:drive_url=dr_r.json().get("url","")
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
                    if evo_r.status_code in(200,201):audio_b64=evo_r.json().get("base64","")
                    else:audio_b64=""
                if audio_b64:
                    audio_bytes=b64mod.b64decode(audio_b64)
                    files={"file":("audio.ogg",iomod.BytesIO(audio_bytes),"audio/ogg")}
                    async with httpx.AsyncClient(timeout=30) as wc:
                        wr=await wc.post("https://api.openai.com/v1/audio/transcriptions",headers={"Authorization":f"Bearer {OPENAI_KEY}"},data={"model":"whisper-1"},files=files)
                        transcription=wr.json().get("text","")
                    if transcription:
                        await guardar_msg(tel,emp.get("id",0),"user",f"[audio] {transcription}")
                        s=BiaState(telefono=tel,mensaje_original=transcription,tipo_mensaje="audio",empleado=emp);s=await procesar(s)
                        if s.respuesta:
                            await wa(f"{tel}@s.whatsapp.net",s.respuesta)
                            await guardar_msg(tel,s.empleado.get("id",0),"assistant",s.respuesta)
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
        # Dedup
        last=await db_get("bia_chat_history",f"telefono=eq.{tel}&role=eq.user&order=created_at.desc&limit=1&select=content")
        if last and last[0].get("content","")==cont:log.info(f"Dup from {tel}");return{"ok":True}
        emps=await db_get("empleados",f"telefono=eq.{tel}&select=*")
        if not emps:await wa(f"{tel}@s.whatsapp.net","No estas registrado.");return{"ok":True}
        emp=emps[0];await guardar_msg(tel,emp.get("id",0),"user",cont)
        s=BiaState(telefono=tel,mensaje_original=cont,tipo_mensaje="texto",empleado=emp);s=await procesar(s)
        if s.respuesta:
            await wa(f"{tel}@s.whatsapp.net",s.respuesta)
            await guardar_msg(tel,s.empleado.get("id",0),"assistant",s.respuesta)
        return{"ok":True,"trace_id":s.trace_id}
    except Exception as e:log.error(f"Webhook: {e}");return{"ok":False}

@app.post("/test")
async def test(req:Request):
    d=await req.json();emp=d.get("empleado",{"id":1,"nombre":"Test","telefono":"0","apodo":"Test","rol":1,"coste_hora":20})
    s=BiaState(telefono=emp.get("telefono","0"),mensaje_original=d.get("mensaje","hola"),empleado=emp);s=await procesar(s)
    return{"trace_id":s.trace_id,"dominio":s.dominio,"dominio_fuente":s.dominio_fuente,"confianza":s.confianza,"respuesta":s.respuesta,"errores":s.errores,"duracion_ms":s.duracion_ms}

@app.get("/health")
async def health():return{"status":"ok","service":"bia-v3","version":"7.1-hardened"}

if __name__=="__main__":
    import uvicorn;uvicorn.run(app,host="0.0.0.0",port=PORT)
