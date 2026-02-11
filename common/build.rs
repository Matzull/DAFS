fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Definimos las rutas relativas
    let proto_file = "../proto/dfs.proto";
    let proto_dir = "../proto"; // El directorio donde buscar los imports

    // 2. Avisamos a cargo que si el archivo cambia, recompile
    println!("cargo:rerun-if-changed={proto_file}");

    // 3. Compilación avanzada
    // .compile( &[archivos], &[includes] )
    tonic_build::configure().compile(
        &[proto_file], // ¿Qué archivo compilar?
        &[proto_dir],  // ¿Dónde buscar ese archivo y sus dependencias?
    )?;

    Ok(())
}
