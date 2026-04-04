use crate::commons::utils::CONFIG as config;
use crate::commons::db;
use crate::commons::redis;
use boa_engine::js_string;
use boa_engine::native_function::NativeFunction;
use boa_engine::object::builtins::JsPromise;
use boa_engine::property::Attribute;
use boa_engine::{Context, JsValue, Source};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AddonScript {
    path: String,
    source: String,
}

lazy_static::lazy_static! {
    static ref ADDON_SCRIPTS: RwLock<Vec<AddonScript>> = RwLock::new(Vec::new());
}

const ADDON_BOOTSTRAP: &str = r#"
globalThis.__addonHandlers = {};
globalThis.query = (name, handler) => {
  globalThis.__addonHandlers[name] = handler;
};
globalThis.db = {
  query: (sql, params) => __db_query(sql, params || []),
};
globalThis.cache = {
  get: (key) => __cache_get(key),
  set: (key, value, ttlSecs) => __cache_set(key, value, ttlSecs),
  del: (key) => __cache_del(key),
};
globalThis.console = {
  log: (...args) => __console_log(...args),
};
"#;

pub fn init_addons() {
    if config.distributed.node_role == "worker" {
        dbg!("addons_waiting_master_sync", &config.distributed.node_id);
        return;
    }

    let scripts = load_addon_scripts();
    replace_addon_scripts(scripts);
}

pub fn addon_scripts_snapshot() -> Value {
    let scripts = addon_scripts();
    json!(scripts)
}

pub fn sync_addon_scripts_from_master(value: &Value) {
    let scripts = serde_json::from_value::<Vec<AddonScript>>(value.clone()).unwrap_or_default();
    replace_addon_scripts(scripts);
}

fn replace_addon_scripts(scripts: Vec<AddonScript>) {
    if scripts.is_empty() {
        dbg!("addons_disabled", "addons directory missing or no js files");
    } else {
        dbg!(
            "addons_loaded",
            scripts
                .iter()
                .map(|script| script.path.as_str())
                .collect::<Vec<_>>()
        );
    }

    if let Ok(mut addon_scripts) = ADDON_SCRIPTS.write() {
        *addon_scripts = scripts;
    }
}

fn addon_scripts() -> Vec<AddonScript> {
    ADDON_SCRIPTS
        .read()
        .map(|scripts| scripts.clone())
        .unwrap_or_default()
}

pub fn has_addons() -> bool {
    !addon_scripts().is_empty()
}

pub fn run_addon_task(task_type: &str, task: &Value) -> Option<Value> {
    if task_type.is_empty() {
        return None;
    }

    for script in addon_scripts().iter() {
        match run_script_task(script, task_type, task) {
            Ok(Some(result)) => return Some(result),
            Ok(None) => {}
            Err(err) => {
                dbg!("addon_script_error", &script.path, task_type, err);
            }
        }
    }

    None
}

fn load_addon_scripts() -> Vec<AddonScript> {
    let addon_dir = Path::new("addons");
    if !addon_dir.is_dir() {
        return Vec::new();
    }

    let Ok(entries) = fs::read_dir(addon_dir) else {
        return Vec::new();
    };

    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(OsStr::new("js")))
        .filter_map(|path| {
            let source = fs::read_to_string(&path).ok()?;
            Some(AddonScript {
                path: path.display().to_string(),
                source,
            })
        })
        .collect()
}

fn run_script_task(
    script: &AddonScript,
    task_type: &str,
    task: &Value,
) -> Result<Option<Value>, String> {
    let mut context = Context::default();
    register_db_functions(&mut context)?;
    context
        .eval(Source::from_bytes(ADDON_BOOTSTRAP))
        .map_err(|err| err.to_string())?;
    context
        .eval(Source::from_bytes(script.source.as_str()))
        .map_err(|err| err.to_string())?;

    let task_type_js = serde_json::to_string(task_type).map_err(|err| err.to_string())?;
    let task_json = serde_json::to_string(task).map_err(|err| err.to_string())?;
    let invoke_source = format!(
        r#"
(() => {{
  const handler = globalThis.__addonHandlers[{task_type_js}];
  if (typeof handler !== "function") {{
    return undefined;
  }}
  return handler(globalThis.db, {task_json});
}})()
"#
    );

    let mut value = context
        .eval(Source::from_bytes(invoke_source.as_str()))
        .map_err(|err| err.to_string())?;
    if let Some(object) = value.as_object() {
        if let Ok(promise) = JsPromise::from_object(object.clone()) {
            value = promise
                .await_blocking(&mut context)
                .map_err(|err| err.to_string())?;
        }
    }

    js_value_to_json(value, &mut context)
}

fn js_value_to_json(value: JsValue, context: &mut Context) -> Result<Option<Value>, String> {
    match value.to_json(context) {
        Ok(Some(json_value)) => Ok(Some(json_value)),
        Ok(None) => Ok(None),
        Err(err) => Ok(Some(json!({
            "ok": false,
            "error": err.to_string(),
        }))),
    }
}

fn register_db_functions(context: &mut Context) -> Result<(), String> {
    context
        .register_global_builtin_callable(
            js_string!("__db_query"),
            2,
            NativeFunction::from_fn_ptr(js_db_query),
        )
        .map_err(|err| err.to_string())?;
    context
        .register_global_builtin_callable(
            js_string!("__cache_get"),
            1,
            NativeFunction::from_fn_ptr(js_cache_get),
        )
        .map_err(|err| err.to_string())?;
    context
        .register_global_builtin_callable(
            js_string!("__cache_set"),
            3,
            NativeFunction::from_fn_ptr(js_cache_set),
        )
        .map_err(|err| err.to_string())?;
    context
        .register_global_builtin_callable(
            js_string!("__cache_del"),
            1,
            NativeFunction::from_fn_ptr(js_cache_del),
        )
        .map_err(|err| err.to_string())?;
    context
        .register_global_builtin_callable(
            js_string!("__console_log"),
            0,
            NativeFunction::from_fn_ptr(js_console_log),
        )
        .map_err(|err| err.to_string())?;
    context
        .register_global_property(js_string!("db"), JsValue::null(), Attribute::all())
        .map_err(|err| err.to_string())?;
    context
        .register_global_property(js_string!("cache"), JsValue::null(), Attribute::all())
        .map_err(|err| err.to_string())
}

fn js_db_query(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> boa_engine::JsResult<JsValue> {
    let sql = js_arg_to_string(args.first(), context);
    let params = js_arg_to_json(args.get(1), context)
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(db::db_query(sql.as_str(), params))
    });

    let value = match result {
        Ok(rows) => json!(rows),
        Err(err) => json!({
            "ok": false,
            "error": err,
        }),
    };

    Ok(json_to_js_value(value, context))
}

fn js_cache_get(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> boa_engine::JsResult<JsValue> {
    let key = js_arg_to_string(args.first(), context);
    let result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(redis::redis_get(key.as_str()))
    });

    match result {
        Ok(Some(value)) => Ok(json_to_js_value(value, context)),
        Ok(None) => Ok(JsValue::undefined()),
        Err(err) => Ok(json_to_js_value(
            json!({
                "ok": false,
                "error": err,
            }),
            context,
        )),
    }
}

fn js_cache_set(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> boa_engine::JsResult<JsValue> {
    let key = js_arg_to_string(args.first(), context);
    let value = js_arg_to_json(args.get(1), context).unwrap_or(Value::Null);
    let ttl_secs = js_arg_to_json(args.get(2), context).and_then(|value| value.as_u64());
    let result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(redis::redis_set(
            key.as_str(),
            &value,
            ttl_secs,
        ))
    });

    match result {
        Ok(()) => Ok(JsValue::new(true)),
        Err(err) => Ok(json_to_js_value(
            json!({
                "ok": false,
                "error": err,
            }),
            context,
        )),
    }
}

fn js_cache_del(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> boa_engine::JsResult<JsValue> {
    let key = js_arg_to_string(args.first(), context);
    let result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(redis::redis_del(key.as_str()))
    });

    match result {
        Ok(()) => Ok(JsValue::new(true)),
        Err(err) => Ok(json_to_js_value(
            json!({
                "ok": false,
                "error": err,
            }),
            context,
        )),
    }
}


fn js_arg_to_string(value: Option<&JsValue>, context: &mut Context) -> String {
    value
        .and_then(|value| value.to_string(context).ok())
        .map(|value| value.to_std_string_lossy())
        .unwrap_or_default()
}

fn js_arg_to_json(value: Option<&JsValue>, context: &mut Context) -> Option<Value> {
    value
        .and_then(|value| value.to_json(context).ok())
        .flatten()
}

fn js_console_log(
    _this: &JsValue,
    args: &[JsValue],
    context: &mut Context,
) -> boa_engine::JsResult<JsValue> {
    let text = args
        .iter()
        .map(|value| js_log_arg_to_string(value, context))
        .collect::<Vec<_>>()
        .join(" ");
    println!("[addon.js] {text}");
    Ok(JsValue::undefined())
}

fn js_log_arg_to_string(value: &JsValue, context: &mut Context) -> String {
    if let Some(json_value) = value.to_json(context).ok().flatten() {
        if json_value.is_string() {
            return json_value.as_str().unwrap_or("").to_string();
        }
        return serde_json::to_string(&json_value).unwrap_or_else(|_| "[object]".to_string());
    }

    value
        .to_string(context)
        .map(|value| value.to_std_string_lossy())
        .unwrap_or_else(|_| "[value]".to_string())
}

fn json_to_js_value(value: Value, context: &mut Context) -> JsValue {
    let Ok(source) = serde_json::to_string(&value) else {
        return JsValue::null();
    };
    let source = format!("({source})");

    context
        .eval(Source::from_bytes(source.as_str()))
        .unwrap_or_else(|_| JsValue::null())
}
