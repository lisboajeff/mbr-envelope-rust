use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("campo obrigatorio ausente ou vazio: {0}")]
    CampoObrigatorio(&'static str),
    #[error("cadeia de trace invalida: {0}")]
    CadeiaTraceInvalida(String),
    #[error("step {0} nao pode conter schema_validation na origem")]
    PreCertificacaoProibida(u32),
    #[error("erro de serializacao JSON do envelope: {0}")]
    SerializacaoJson(String),
}
