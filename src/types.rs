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
}

fn validar_nao_vazio(valor: String, campo: &'static str) -> Result<String, EnvelopeError> {
    let limpo = valor.trim();
    if limpo.is_empty() {
        return Err(EnvelopeError::CampoObrigatorio(campo));
    }
    Ok(limpo.to_string())
}
