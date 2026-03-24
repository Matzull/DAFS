import pty
import os
import subprocess
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import re

# --- CONFIGURACIÓN DE LAS PRUEBAS ---
DFS_PATH = "/tmp/DAFS/testfile"
NATIVE_PATH = "/tmp/testfile"
BLOCK_SIZES = ["64k", "256k", "1m"]
FILE_SIZES = ["64m", "512m"]
# BLOCK_SIZES = ["256k", "1m"]
# FILE_SIZES = ["64m"]

def run_iozone_benchmark(target_path, label):
    all_results = []
    print(f"\n{'='*60}")
    print(f"🚀 INICIANDO TEST: {label}")
    print(f"{'='*60}")

    for f_size in FILE_SIZES:
        # Calculamos los KB esperados para identificar la línea de datos
        multiplier = 1024 if 'm' in f_size.lower() else 1024 * 1024
        size_kb = int(re.search(r'\d+', f_size).group()) * multiplier

        for b_size in BLOCK_SIZES:
            print(f"\n[SISTEMA: {label}] | Archivo: {f_size} | Bloque: {b_size}")
            
            cmd = ["iozone", "-e", "-+u", "-i", "0", "-i", "1", "-i", "2", 
                   "-r", b_size, "-s", f_size, "-f", target_path]

            master_fd, slave_fd = pty.openpty()
            process = subprocess.Popen(cmd, stdout=slave_fd, stderr=slave_fd, text=True)
            os.close(slave_fd)

            output_buffer = ""
            try:
                started_table = False
                while True:
                    try:
                        output = os.read(master_fd, 1024).decode()
                        if "random" in output.lower():
                            started_table = True
                            output = output.split("random", 1)[1]  # Ignorar encabezados previos
                            output = 69*" " + output  # Alinear con el formato esperado
                        if not output:
                            break
                        if not started_table:
                            continue
                        
                        output_buffer += output
                        print(output, end='')
                                
                    except OSError: break
            finally:
                process.wait()
                os.close(master_fd)
                if os.path.exists(target_path):
                    os.remove(target_path)

            # PARSEAR RESULTADOS
            for line in output_buffer.splitlines():
                parts = line.split()
                if len(parts) >= 7 and parts[0].isdigit():
                    if int(parts[0]) == size_kb:
                        all_results.append({
                            'Sistema': label,
                            'File_Size': f_size,
                            'Block_Size': b_size,
                            'Write': int(parts[2]),
                            'Read': int(parts[4]),
                            'Random_RW': int(parts[6])
                        })
                        break
    return all_results

def generate_visual_report(df):
    if df.empty:
        print("\n❌ Error: No se pudieron recolectar datos.")
        return

    print("\n" + "="*60)
    print("📊 GENERANDO PANEL INTERACTIVO (PLOTLY)...")
    print("="*60)

    # Creamos subplots: 1 fila, 3 columnas
    fig = make_subplots(
        rows=1, cols=3, 
        subplot_titles=("Escritura Secuencial", "Lectura Secuencial", "I/O Aleatorio"),
        horizontal_spacing=0.1
    )

    metrics = ['Write', 'Read', 'Random_RW']

    # Si hay varios tamaños de archivo, agregamos por sistema y bloque
    # para evitar barras superpuestas en la misma categoria X.
    df_plot_base = (
        df.groupby(['Sistema', 'Block_Size'], as_index=False, observed=False)[metrics]
        .mean()
        .sort_values('Block_Size')
    )

    colors = {'Linux_Nativo': '#636EFA', 'DFS_Distribuido': '#EF553B'}
    
    for i, metric in enumerate(metrics, 1):
        for sistema in df_plot_base['Sistema'].unique():
            df_plot = df_plot_base[df_plot_base['Sistema'] == sistema]
            fig.add_trace(
                go.Bar(
                    x=df_plot['Block_Size'],
                    y=df_plot[metric],
                    name=sistema,
                    marker_color=colors.get(sistema),
                    showlegend=(i == 1) # Solo mostrar leyenda en el primer gráfico
                ),
                row=1, col=i
            )

    file_sizes = [str(size) for size in df['File_Size'].dropna().unique()]
    if len(file_sizes) == 1:
        title_suffix = f"Tamaño Archivo: {file_sizes[0]}"
    else:
        title_suffix = "Promedio sobre varios tamanos de archivo"

    fig.update_layout(
        title_text=f"Comparativa de Rendimiento: DFS vs Nativo ({title_suffix})",
        template="plotly_white",
        barmode='group',
        height=500
    )
    
    # Actualizar títulos de ejes
    fig.update_yaxes(title_text="KB/segundo")
    fig.update_xaxes(title_text="Tamaño de Bloque")

    # Guardar y mostrar
    output_html = 'reporte_rendimiento.html'
    fig.write_html(output_html)
    print(f"\n✅ Reporte interactivo guardado en: {output_html}")
    fig.show()

def generate_text_report(df):
    print("\n" + "="*60)
    print("📋 REPORTE DE RENDIMIENTO (TEXTO)")
    print("="*60)
    for sistema in df['Sistema'].unique():
        print(f"\n🔹 {sistema} 🔹")
        df_sis = df[df['Sistema'] == sistema]
        for _, row in df_sis.iterrows():
            print(f"Archivo: {row['File_Size']} | Bloque: {row['Block_Size']} | "
                  f"Write: {row['Write']} KB/s | Read: {row['Read']} KB/s | "
                  f"Random_RW: {row['Random_RW']} KB/s")

if __name__ == "__main__":
    # Ejecución de benchmarks
    results_native = run_iozone_benchmark(NATIVE_PATH, "Linux_Nativo")
    results_dfs = run_iozone_benchmark(DFS_PATH, "DFS_Distribuido")
    
    # Consolidación de datos
    df_total = pd.DataFrame(results_native + results_dfs)
    
    # Asegurar orden lógico
    df_total['Block_Size'] = pd.Categorical(df_total['Block_Size'], categories=BLOCK_SIZES, ordered=True)
    
    generate_visual_report(df_total)
    generate_text_report(df_total)