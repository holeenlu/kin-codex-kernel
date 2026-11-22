//! JSON 物件的位元組級編修：只動指定的頂層成員，其餘位元組（鍵順序、空白、跳脫寫法、數值字面值）原樣保留。
//!
//! 請求本體與 `x-codex-turn-metadata` 的欄位順序是下游客戶端的原樣，重新序列化本身就是可辨識的差異
//! （見 `tap_contract::envelope`），所以這裡不經過 `serde_json::Value` 或 `BTreeMap`。
//! 鍵以解碼後的名稱比對，`"client_metadata"` 這類跳脫寫法也認得；同名鍵重複時每一個都處理。

use std::fmt;

use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, Visitor};
use serde_json::Value;
use serde_json::value::RawValue;

/// 依原順序的頂層成員：解碼後的鍵，以及借自原文的值。
struct Members<'a>(Vec<(String, &'a RawValue)>);

impl<'de> Deserialize<'de> for Members<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visit;

        impl<'de> Visitor<'de> for Visit {
            type Value = Members<'de>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut members = Vec::new();
                while let Some(entry) = map.next_entry::<String, &'de RawValue>()? {
                    members.push(entry);
                }
                Ok(Members(members))
            }
        }

        deserializer.deserialize_map(Visit)
    }
}

/// 解析過的 JSON 物件；值的位置借自原文。
#[derive(Debug)]
pub(crate) struct Object<'a> {
    raw: &'a str,
    /// `{` 的位置。
    open: usize,
    members: Vec<(String, &'a RawValue)>,
}

impl<'a> Object<'a> {
    /// `raw` 不是 JSON 物件時回 `None`。
    pub(crate) fn parse(raw: &'a str) -> Option<Self> {
        let Members(members) = serde_json::from_str(raw).ok()?;
        Some(Self {
            raw,
            open: raw.len() - raw.trim_start().len(),
            members,
        })
    }

    pub(crate) fn contains(&self, key: &str) -> bool {
        self.members.iter().any(|(name, _)| name == key)
    }

    /// 這個鍵的每一個值，依原順序。
    pub(crate) fn values(&self, key: &str) -> Vec<&'a RawValue> {
        self.members
            .iter()
            .filter_map(|(name, value)| (name == key).then_some(*value))
            .collect()
    }

    /// 這個鍵的值（重複時取最後一個，與多數解析器相同）。
    pub(crate) fn get(&self, key: &str) -> Option<&'a RawValue> {
        self.members
            .iter()
            .rev()
            .find_map(|(name, value)| (name == key).then_some(*value))
    }

    /// 值在原文中的範圍。
    fn span(&self, value: &RawValue) -> Option<(usize, usize)> {
        let start = (value.get().as_ptr() as usize).checked_sub(self.raw.as_ptr() as usize)?;
        let end = start.checked_add(value.get().len())?;
        (end <= self.raw.len()).then_some((start, end))
    }

    /// 刪掉這個鍵的每一個成員；不存在時回 `None`。
    pub(crate) fn remove(&self, key: &str) -> Option<String> {
        if !self.contains(key) {
            return None;
        }
        let raw = self.raw;
        let mut out = String::with_capacity(raw.len());
        out.push_str(raw.get(..=self.open)?);
        // `cursor` 之前的原文都已處理；每個成員的片段是「上一個值的結尾」到「自己的值的結尾」，
        // 也就是 `, "鍵": 值`（第一個成員沒有逗號）。
        let mut cursor = self.open + 1;
        let mut emitted = false;
        for (index, (name, value)) in self.members.iter().enumerate() {
            let (_, end) = self.span(value)?;
            if name != key {
                let segment = raw.get(cursor..end)?;
                if !emitted && index > 0 {
                    // 前面的成員都刪掉了：這段開頭的逗號跟著拿掉。
                    let comma = segment.find(',')?;
                    out.push_str(segment.get(comma + 1..)?);
                } else {
                    out.push_str(segment);
                }
                emitted = true;
            }
            cursor = end;
        }
        out.push_str(raw.get(cursor..)?);
        Some(out)
    }

    /// 把這個鍵的值換成 `value`（必須是合法的 JSON 文字）：存在時就地替換每一個，否則加在最後。
    pub(crate) fn set(&self, key: &str, value: &str) -> Option<String> {
        let mut hits = self
            .members
            .iter()
            .filter(|(name, _)| name == key)
            .map(|(_, value)| self.span(value))
            .peekable();
        if hits.peek().is_none() {
            return append(self.raw, key, value);
        }
        let mut out = String::with_capacity(self.raw.len() + value.len());
        let mut cursor = 0;
        for hit in hits {
            let (start, end) = hit?;
            out.push_str(self.raw.get(cursor..start)?);
            out.push_str(value);
            cursor = end;
        }
        out.push_str(self.raw.get(cursor..)?);
        Some(out)
    }
}

/// 在物件最後加一個成員，不解析整個物件（呼叫端已知道這個鍵不存在）。`raw` 不像物件時回 `None`。
pub(crate) fn append(raw: &str, key: &str, value: &str) -> Option<String> {
    let body = raw.trim_end();
    if !body.trim_start().starts_with('{') {
        return None;
    }
    let prefix = body.strip_suffix('}')?;
    // 值的最後一個字元不可能是 `{`，所以去掉空白後以 `{` 結尾就表示是空物件。
    let separator = if prefix.trim_end().ends_with('{') { "" } else { "," };
    let key = string(key);
    let tail = raw.get(body.len()..)?;
    Some(format!("{prefix}{separator}{key}:{value}}}{tail}"))
}

/// 刪掉 `raw` 物件裡這個鍵；不是物件或沒有這個鍵時回 `None`。
pub(crate) fn remove(raw: &str, key: &str) -> Option<String> {
    Object::parse(raw)?.remove(key)
}

/// 設定 `raw` 物件裡這個鍵的值；不是物件時回 `None`。
pub(crate) fn set(raw: &str, key: &str, value: &str) -> Option<String> {
    Object::parse(raw)?.set(key, value)
}

/// 字串的 JSON 文字。
pub(crate) fn string(value: &str) -> String {
    Value::String(value.to_owned()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removal_keeps_every_other_byte() {
        let cases = [
            (r#"{"a":1, "x": [ 1 ],"b":2}"#, r#"{"a":1,"b":2}"#),
            (r#"{ "x":1 , "a" : 2 }"#, r#"{ "a" : 2 }"#),
            (r#"{"a":1,"x":{"y":"}"}}"#, r#"{"a":1}"#),
            (r#"{ "x": null }"#, r#"{ }"#),
            (r#"{"x":1,"x":2,"a":3,"x":4}"#, r#"{"a":3}"#),
            (r#"{"x":1,"a":"x"}"#, r#"{"a":"x"}"#),
        ];
        for (raw, expected) in cases {
            assert_eq!(remove(raw, "x").as_deref(), Some(expected), "{raw}");
            let _: Value = serde_json::from_str(expected).unwrap();
        }
        assert_eq!(remove(r#"{"a":1}"#, "x"), None);
        assert_eq!(remove("[1]", "x"), None);
    }

    #[test]
    fn set_replaces_in_place_or_appends_last() {
        assert_eq!(
            set(r#"{"z":1, "x" :"old","a":2}"#, "x", r#""new""#).as_deref(),
            Some(r#"{"z":1, "x" :"new","a":2}"#)
        );
        assert_eq!(
            set(r#"{"x":"old","x":"dup"}"#, "x", "0").as_deref(),
            Some(r#"{"x":0,"x":0}"#)
        );
        assert_eq!(
            set(r#"{"z":1} "#, "x", "true").as_deref(),
            Some(r#"{"z":1,"x":true} "#)
        );
        assert_eq!(set("{ }", "x", "1").as_deref(), Some(r#"{ "x":1}"#));
        assert_eq!(set(r#""{}""#, "x", "1"), None);
        assert_eq!(append("[1]", "x", "1"), None);
    }

    #[test]
    fn get_reads_the_last_duplicate_and_decodes_keys() {
        // 第二個鍵是跳脫寫法（`m` 寫成反斜線 u006d），解碼後同名。
        let escaped = format!("client_{}u006detadata", char::from(92));
        let raw = format!(r#"{{"client_metadata":{{"a":1}},"{escaped}":{{"b":2}}}}"#);
        let object = Object::parse(&raw).unwrap();
        assert!(object.contains("client_metadata"));
        assert_eq!(object.values("client_metadata").len(), 2);
        assert_eq!(object.get("client_metadata").unwrap().get(), r#"{"b":2}"#);
        assert!(object.get("other").is_none());
        // 每一個同名鍵都就地替換，鍵本身的位元組不變。
        assert_eq!(
            object.set("client_metadata", "{}"),
            Some(format!(r#"{{"client_metadata":{{}},"{escaped}":{{}}}}"#))
        );
    }
}
