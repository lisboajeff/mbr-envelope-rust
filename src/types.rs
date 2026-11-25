use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::EnvelopeError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaValidation {
    pub validated: bool,
    pub schema_name: String,
    pub schema_version: String,
    pub validated_at: DateTime<Utc>,
    pub validator_service_name: String,
    pub validator_process_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceStep {
    pub step: u32,
    pub span_id: String,
    pub service_name: String,
    pub process_id: String,
    pub executed_at: DateTime<Utc>,
    pub responsibility: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_validation: Option<SchemaValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeMetadata {
    pub topic: String,
    pub schema_name: String,
    pub schema_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextoLogEnvelope {
    pub event_id: Uuid,
    pub process_instance_id: Uuid,
    pub topic: String,
    pub schema_name: String,
    pub schema_version: String,
    pub step_atual: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FalhaEstruturalEvento {
    pub codigo: String,
    pub mensagem: String,
    pub violacoes: Vec<String>,
    pub exchange_origem: String,
    pub routing_key: String,
    pub detectado_em: DateTime<Utc>,
    pub payload_original: serde_json::Value,
}

impl FalhaEstruturalEvento {
    #[must_use]
    pub fn nova(
        codigo: impl Into<String>,
        mensagem: impl Into<String>,
        violacoes: Vec<String>,
        exchange_origem: impl Into<String>,
        routing_key: impl Into<String>,
        payload_original: serde_json::Value,
    ) -> Self {
        Self {
            codigo: codigo.into(),
            mensagem: mensagem.into(),
            violacoes,
            exchange_origem: exchange_origem.into(),
            routing_key: routing_key.into(),
            detectado_em: Utc::now(),
            payload_original,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub event_id: Uuid,
    pub process_instance_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub metadata: EnvelopeMetadata,
    pub trace: Vec<TraceStep>,
    pub data: T,
}

impl<T> Envelope<T> {
    /// Constroi o envelope inicial (`step = 1`) sem `schema_validation`.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::CampoObrigatorio` se algum identificador ou metadado estiver vazio.
    pub fn criar_inicial(
        topic: impl Into<String>,
        schema_name: impl Into<String>,
        schema_version: impl Into<String>,
        service_name: impl Into<String>,
        process_id: impl Into<String>,
        responsibility: impl Into<String>,
        data: T,
    ) -> Result<Self, EnvelopeError> {
        let topic = validar_nao_vazio(topic.into(), "metadata.topic")?;
        let schema_name = validar_nao_vazio(schema_name.into(), "metadata.schema_name")?;
        let schema_version = validar_nao_vazio(schema_version.into(), "metadata.schema_version")?;
        let service_name = validar_nao_vazio(service_name.into(), "trace.service_name")?;
        let process_id = validar_nao_vazio(process_id.into(), "trace.process_id")?;
        let responsibility = validar_nao_vazio(responsibility.into(), "trace.responsibility")?;

        let agora = Utc::now();
        Ok(Self {
            event_id: Uuid::new_v4(),
            process_instance_id: Uuid::new_v4(),
            timestamp: agora,
            metadata: EnvelopeMetadata {
                topic,
                schema_name,
                schema_version,
            },
            trace: vec![TraceStep {
                step: 1,
                span_id: Uuid::new_v4().to_string(),
                service_name,
                process_id,
                executed_at: agora,
                responsibility,
                schema_validation: None,
            }],
            data,
        })
    }

    /// Avanca o envelope para um novo topico/schema adicionando o proximo `step` sem `schema_validation`.
    ///
    /// # Errors
    /// Retorna erro se a cadeia anterior estiver vazia, se algum step anterior nao estiver certificado
    /// ou se algum campo obrigatorio estiver vazio.
    pub fn avancar_step<U>(
        self,
        destino: EnvelopeMetadata,
        service_name: impl Into<String>,
        process_id: impl Into<String>,
        responsibility: impl Into<String>,
        novos_dados: U,
    ) -> Result<Envelope<U>, EnvelopeError> {
        if self.trace.is_empty() {
            return Err(EnvelopeError::CadeiaTraceInvalida(
                "envelope anterior possui trace vazio".to_string(),
            ));
        }
        for s in &self.trace {
            let valido = s.schema_validation.as_ref().is_some_and(|v| v.validated);
            if !valido {
                return Err(EnvelopeError::CadeiaTraceInvalida(format!(
                    "step {} nao esta certificado antes de avancar",
                    s.step
                )));
            }
        }

        let novo_topic = validar_nao_vazio(destino.topic, "metadata.topic")?;
        let novo_schema_name = validar_nao_vazio(destino.schema_name, "metadata.schema_name")?;
        let novo_schema_version =
            validar_nao_vazio(destino.schema_version, "metadata.schema_version")?;
        let service_name = validar_nao_vazio(service_name.into(), "trace.service_name")?;
        let process_id = validar_nao_vazio(process_id.into(), "trace.process_id")?;
        let responsibility = validar_nao_vazio(responsibility.into(), "trace.responsibility")?;

        let proximo_step = self.trace.last().map_or(1, |s| s.step + 1);
        let mut trace = self.trace;
        let agora = Utc::now();
        trace.push(TraceStep {
            step: proximo_step,
            span_id: Uuid::new_v4().to_string(),
            service_name,
            process_id,
            executed_at: agora,
            responsibility,
            schema_validation: None,
        });

        Ok(Envelope {
            event_id: Uuid::new_v4(),
            process_instance_id: self.process_instance_id,
            timestamp: agora,
            metadata: EnvelopeMetadata {
                topic: novo_topic,
                schema_name: novo_schema_name,
                schema_version: novo_schema_version,
            },
            trace,
            data: novos_dados,
        })
    }

    /// Valida a cadeia de trace para o Validador Central (steps anteriores certificados; ultimo sem certificacao).
    ///
    /// # Errors
    /// Retorna `EnvelopeError` caso o trace esteja vazio, falte certificacao anterior ou o ultimo step venha pre-certificado.
    pub fn validar_para_certificacao(&self) -> Result<(), EnvelopeError> {
        let len = self.trace.len();
        if len == 0 {
            return Err(EnvelopeError::CadeiaTraceInvalida(
                "trace nao pode ser vazio".to_string(),
            ));
        }
        for step in &self.trace[..len - 1] {
            if !step.schema_validation.as_ref().is_some_and(|v| v.validated) {
                return Err(EnvelopeError::CadeiaTraceInvalida(format!(
                    "step anterior {} sem certificacao",
                    step.step
                )));
            }
        }
        if let Some(ultimo) = self.trace.last()
            && ultimo.schema_validation.is_some()
        {
            return Err(EnvelopeError::PreCertificacaoProibida(ultimo.step));
        }
        Ok(())
    }

    /// Valida que todos os steps do envelope (inclusive o ultimo) foram certificados pelo Validador Central.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::CadeiaTraceInvalida` se o trace estiver vazio ou houver step sem certificacao valida.
    pub fn validar_certificado(&self) -> Result<(), EnvelopeError> {
        if self.trace.is_empty() {
            return Err(EnvelopeError::CadeiaTraceInvalida(
                "trace nao pode ser vazio".to_string(),
            ));
        }
        for step in &self.trace {
            if !step.schema_validation.as_ref().is_some_and(|v| v.validated) {
                return Err(EnvelopeError::CadeiaTraceInvalida(format!(
                    "step {} nao esta certificado",
                    step.step
                )));
            }
        }
        Ok(())
    }

    /// Valida a certificacao do envelope e extrai o payload `data` com seguranca.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se o envelope nao estiver totalmente certificado.
    pub fn extrair_data(self) -> Result<T, EnvelopeError> {
        self.validar_certificado()?;
        Ok(self.data)
    }

    /// Valida a certificacao do envelope e retorna referencia ao payload `data`.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se o envelope nao estiver totalmente certificado.
    pub fn extrair_data_ref(&self) -> Result<&T, EnvelopeError> {
        self.validar_certificado()?;
        Ok(&self.data)
    }

    /// Certifica o ultimo `step` do `trace` apos validacao pelo Validador Central.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se a cadeia estiver invalida, se o ultimo step ja estiver certificado
    /// ou se `validator_service_name` / `validator_process_id` estiverem vazios.
    pub fn certificar_ultimo_step(
        &mut self,
        validator_service_name: impl Into<String>,
        validator_process_id: impl Into<String>,
    ) -> Result<(), EnvelopeError> {
        self.validar_para_certificacao()?;
        let service_name =
            validar_nao_vazio(validator_service_name.into(), "schema_validation.validator_service_name")?;
        let process_id =
            validar_nao_vazio(validator_process_id.into(), "schema_validation.validator_process_id")?;

        let schema_name = self.metadata.schema_name.clone();
        let schema_version = self.metadata.schema_version.clone();
        if let Some(ultimo) = self.trace.last_mut() {
            ultimo.schema_validation = Some(SchemaValidation {
                validated: true,
                schema_name,
                schema_version,
                validated_at: Utc::now(),
                validator_service_name: service_name,
                validator_process_id: process_id,
            });
        }
        Ok(())
    }

    /// Extrai campos padronizados de correlacao distribuida para logs estruturados.
    #[must_use]
    pub fn contexto_log(&self) -> ContextoLogEnvelope {
        ContextoLogEnvelope {
            event_id: self.event_id,
            process_instance_id: self.process_instance_id,
            topic: self.metadata.topic.clone(),
            schema_name: self.metadata.schema_name.clone(),
            schema_version: self.metadata.schema_version.clone(),
            step_atual: self.trace.last().map_or(0, |s| s.step),
        }
    }

    /// Transforma o payload `data` preservando metadados, IDs e cadeia `trace`.
    pub fn map_data<U>(self, f: impl FnOnce(T) -> U) -> Envelope<U> {
        Envelope {
            event_id: self.event_id,
            process_instance_id: self.process_instance_id,
            timestamp: self.timestamp,
            metadata: self.metadata,
            trace: self.trace,
            data: f(self.data),
        }
    }

    /// Deserializa o envelope completo a partir de bytes JSON.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::SerializacaoJson` se o JSON for invalido para `Envelope<T>`.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, EnvelopeError>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_slice(bytes)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))
    }

    /// Deserializa o envelope completo a partir de string JSON.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::SerializacaoJson` se o JSON for invalido para `Envelope<T>`.
    pub fn from_json_str(json: &str) -> Result<Self, EnvelopeError>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_str(json)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))
    }

    /// Serializa o envelope completo para bytes JSON.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::SerializacaoJson` se a serializacao falhar.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, EnvelopeError>
    where
        T: Serialize,
    {
        serde_json::to_vec(self).map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))
    }

    /// Converte o payload `data` para `serde_json::Value` preservando o envelope.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::SerializacaoJson` se a conversao falhar.
    pub fn into_value(self) -> Result<Envelope<serde_json::Value>, EnvelopeError>
    where
        T: Serialize,
    {
        let data = serde_json::to_value(&self.data)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))?;
        Ok(Envelope {
            event_id: self.event_id,
            process_instance_id: self.process_instance_id,
            timestamp: self.timestamp,
            metadata: self.metadata,
            trace: self.trace,
            data,
        })
    }

    /// Converte o payload `data` para um tipo `U` via JSON.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::SerializacaoJson` se a conversao falhar.
    pub fn parse_data<U: serde::de::DeserializeOwned>(self) -> Result<Envelope<U>, EnvelopeError>
    where
        T: Serialize,
    {
        let val = serde_json::to_value(&self.data)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))?;
        let parsed: U = serde_json::from_value(val)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))?;
        Ok(Envelope {
            event_id: self.event_id,
            process_instance_id: self.process_instance_id,
            timestamp: self.timestamp,
            metadata: self.metadata,
            trace: self.trace,
            data: parsed,
        })
    }
}

/// Alias semântico para metadados de destino na emissão/evolução de envelopes.
pub type EnvelopeDestino = EnvelopeMetadata;

/// Emissor de envelopes configurado uma única vez com a identidade do serviço/pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeEmitter {
    service_name: String,
    process_id: String,
}

impl EnvelopeEmitter {
    /// Cria um emissor validando `service_name` e `process_id`.
    ///
    /// # Errors
    /// Retorna `EnvelopeError::CampoObrigatorio` se algum identificador estiver vazio.
    pub fn new(
        service_name: impl Into<String>,
        process_id: impl Into<String>,
    ) -> Result<Self, EnvelopeError> {
        Ok(Self {
            service_name: validar_nao_vazio(service_name.into(), "trace.service_name")?,
            process_id: validar_nao_vazio(process_id.into(), "trace.process_id")?,
        })
    }

    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    #[must_use]
    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    /// Cria um envelope inicial (`step = 1`) usando a identidade do serviço configurada.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se destino ou `responsibility` estiverem vazios.
    pub fn iniciar<T>(
        &self,
        destino: EnvelopeDestino,
        responsibility: impl Into<String>,
        data: T,
    ) -> Result<Envelope<T>, EnvelopeError> {
        Envelope::criar_inicial(
            destino.topic,
            destino.schema_name,
            destino.schema_version,
            &self.service_name,
            &self.process_id,
            responsibility,
            data,
        )
    }

    /// Avança o envelope certificado para o próximo step com a identidade do serviço configurada.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se a cadeia anterior não estiver certificada ou algum campo estiver vazio.
    pub fn avancar<T, U>(
        &self,
        anterior: Envelope<T>,
        destino: EnvelopeDestino,
        responsibility: impl Into<String>,
        novos_dados: U,
    ) -> Result<Envelope<U>, EnvelopeError> {
        anterior.avancar_step(
            destino,
            &self.service_name,
            &self.process_id,
            responsibility,
            novos_dados,
        )
    }

    /// Avança o envelope certificado serializando `novos_dados` para `serde_json::Value`.
    ///
    /// # Errors
    /// Retorna `EnvelopeError` se a cadeia anterior for inválida ou a serialização falhar.
    pub fn avancar_value<T, U: Serialize>(
        &self,
        anterior: Envelope<T>,
        destino: EnvelopeDestino,
        responsibility: impl Into<String>,
        novos_dados: &U,
    ) -> Result<Envelope<serde_json::Value>, EnvelopeError> {
        let val = serde_json::to_value(novos_dados)
            .map_err(|e| EnvelopeError::SerializacaoJson(e.to_string()))?;
        self.avancar(anterior, destino, responsibility, val)
    }
}

fn validar_nao_vazio(valor: String, campo: &'static str) -> Result<String, EnvelopeError> {
    let limpo = valor.trim();
    if limpo.is_empty() {
        return Err(EnvelopeError::CampoObrigatorio(campo));
    }
    Ok(limpo.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn emitter_deve_iniciar_certificar_e_avancar_para_value() {
        let emitter = EnvelopeEmitter::new("ativos.extrator", "pod-1").expect("emitter");
        let destino_1 = EnvelopeDestino {
            topic: "topic.ativos.acoes.listada".to_string(),
            schema_name: "schema.ativos.acoes.listada".to_string(),
            schema_version: "1.0.0".to_string(),
        };
        let mut env = emitter
            .iniciar(destino_1, "listagem", json!({"ticker": "BBAS3"}))
            .expect("inicial");

        env.certificar_ultimo_step("infraestrutura.validador-central", "val-1")
            .expect("certificado");

        let destino_2 = EnvelopeDestino {
            topic: "topic.ativos.cotacao.atualizada".to_string(),
            schema_name: "schema.ativos.cotacao.atualizada".to_string(),
            schema_version: "1.0.0".to_string(),
        };
        let env2 = emitter
            .avancar_value(env, destino_2, "cotacao", &json!({"preco": "14.20"}))
            .expect("avancado");

        assert_eq!(env2.trace.len(), 2);
        assert_eq!(env2.trace[1].service_name, "ativos.extrator");
        let bytes = env2.to_json_bytes().expect("json bytes");
        let parsed: Envelope<serde_json::Value> =
            Envelope::from_json_bytes(&bytes).expect("roundtrip");
        assert_eq!(parsed.data["preco"], "14.20");
    }
}
