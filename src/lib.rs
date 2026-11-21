#![deny(clippy::all)]

pub mod error;
pub mod types;

pub use error::EnvelopeError;
pub use types::{Envelope, EnvelopeMetadata, SchemaValidation, TraceStep};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn deve_criar_envelope_inicial_sem_schema_validation() {
        let env = Envelope::criar_inicial(
            "topic.extracao.ativos.iniciar",
            "extracao-solicitada",
            "1.0.0",
            "infraestrutura.mbr-dispatcher",
            "pod-1",
            "disparo_agendado",
            json!({"ticker": "PETR4"}),
        )
        .expect("envelope valido");

        assert_eq!(env.trace.len(), 1);
        assert_eq!(env.trace[0].step, 1);
        assert!(env.trace[0].schema_validation.is_none());

        let json_str = serde_json::to_string(&env).expect("serializar");
        assert!(!json_str.contains("schema_validation"));
    }

    #[test]
    fn deve_rejeitar_avanco_quando_step_anterior_nao_esta_certificado() {
        let env = Envelope::criar_inicial(
            "topic.a",
            "s",
            "1.0.0",
            "infraestrutura.mbr-dispatcher",
            "pod-1",
            "disparo",
            json!({}),
        )
        .expect("envelope valido");

        let err = env
            .avancar_step(
                EnvelopeMetadata {
                    topic: "topic.b".to_string(),
                    schema_name: "s2".to_string(),
                    schema_version: "1.0.0".to_string(),
                },
                "extracao.extrator",
                "pod-2",
                "extrair",
                json!({}),
            )
            .expect_err("nao pode avancar sem certificacao");

        assert!(matches!(err, EnvelopeError::CadeiaTraceInvalida(_)));
    }

    #[test]
    fn deve_avancar_step_quando_anterior_certificado() {
        let mut env = Envelope::criar_inicial(
            "topic.a",
            "s",
            "1.0.0",
            "infraestrutura.mbr-dispatcher",
            "pod-1",
            "disparo",
            json!({"x": 1}),
        )
        .expect("envelope valido");

        env.trace[0].schema_validation = Some(SchemaValidation {
            validated: true,
            schema_name: "s".to_string(),
            schema_version: "1.0.0".to_string(),
            validated_at: Utc::now(),
            validator_service_name: "infraestrutura.validador-central".to_string(),
            validator_process_id: "val-1".to_string(),
        });

        let pid_original = env.process_instance_id;
        let proximo = env
            .avancar_step(
                EnvelopeMetadata {
                    topic: "topic.b".to_string(),
                    schema_name: "s2".to_string(),
                    schema_version: "1.0.0".to_string(),
                },
                "extracao.extrator",
                "pod-2",
                "extrair",
                json!({"y": 2}),
            )
            .expect("avanco valido");

        assert_eq!(proximo.process_instance_id, pid_original);
        assert_eq!(proximo.trace.len(), 2);
        assert_eq!(proximo.trace[1].step, 2);
        assert!(proximo.trace[1].schema_validation.is_none());
        assert!(proximo.validar_para_certificacao().is_ok());
        assert!(proximo.validar_certificado().is_err());
    }

    #[test]
    fn deve_validar_certificado_e_extrair_data_com_seguranca() {
        let mut env = Envelope::criar_inicial(
            "topic.a",
            "s",
            "1.0.0",
            "infraestrutura.mbr-dispatcher",
            "pod-1",
            "disparo",
            json!({"ticker": "VALE3"}),
        )
        .expect("envelope valido");

        assert!(env.extrair_data_ref().is_err());

        env.trace[0].schema_validation = Some(SchemaValidation {
            validated: true,
            schema_name: "s".to_string(),
            schema_version: "1.0.0".to_string(),
            validated_at: Utc::now(),
            validator_service_name: "infraestrutura.validador-central".to_string(),
            validator_process_id: "val-1".to_string(),
        });

        let data = env.extrair_data().expect("envelope certificado");
        assert_eq!(data["ticker"], "VALE3");
    }

    #[test]
    fn deve_certificar_ultimo_step_e_bloquear_dupla_certificacao() {
        let mut env = Envelope::criar_inicial(
            "topic.a",
            "s",
            "1.0.0",
            "infraestrutura.mbr-dispatcher",
            "pod-1",
            "disparo",
            json!({"ticker": "ITUB4"}),
        )
        .expect("envelope valido");

        env.certificar_ultimo_step("infraestrutura.validador-central", "val-1")
            .expect("certificacao valida");
        assert!(env.validar_certificado().is_ok());

        let err = env
            .certificar_ultimo_step("infraestrutura.validador-central", "val-1")
            .expect_err("nao pode certificar duas vezes");
        assert!(matches!(err, EnvelopeError::PreCertificacaoProibida(1)));
    }
}
