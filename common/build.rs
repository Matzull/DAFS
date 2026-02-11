fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Definimos las rutas relativas
    let proto_file = "../proto/dfs.proto";
    let proto_dir = "../proto"; // El directorio donde buscar los imports

    // 2. Avisamos a cargo que si el archivo cambia, recompile
    println!("cargo:rerun-if-changed={proto_file}");

    // 3. Compilación avanzada
    // .compile( &[archivos], &[includes] )
    tonic_build::configure()
        .field_attribute(
            "common.dfs.WriteBlockRequest.chunk_data",
            "#[prost(bytes=\"bytes\")]",
        )
        // Y por si acaso, para el de lectura también
        .field_attribute(
            "common.dfs.ReadBlockResponse.chunk_data",
            "#[prost(bytes=\"bytes\")]",
        )
        .compile(
            &[proto_file], // ¿Qué archivo compilar?
            &[proto_dir],  // ¿Dónde buscar ese archivo y sus dependencias?
        )?;

    Ok(())
}
