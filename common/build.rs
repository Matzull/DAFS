fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Definimos las rutas relativas
    let dfs_proto_file = "../proto/dfs.proto";
    let raft_proto_file = "../proto/raft.proto";
    let proto_dir = "../proto"; // El directorio donde buscar los imports

    // 2. Avisamos a cargo que si el archivo cambia, recompile
    println!("cargo:rerun-if-changed={dfs_proto_file}");
    println!("cargo:rerun-if-changed={raft_proto_file}");

    // 3. Compilación avanzada
    // .compile( &[archivos], &[includes] )
    tonic_prost_build::configure()
        .field_attribute(
            "common.dfs.WriteBlockRequest.chunk_data",
            "#[prost(bytes=\"bytes\")]",
        )
        // Y por si acaso, para el de lectura también
        .field_attribute(
            "common.dfs.ReadBlockResponse.chunk_data",
            "#[prost(bytes=\"bytes\")]",
        )
        .compile_protos(
            &[dfs_proto_file, raft_proto_file], // ¿Qué archivo compilar?
            &[proto_dir],                       // ¿Dónde buscar ese archivo y sus dependencias?
        )?;

    Ok(())
}
