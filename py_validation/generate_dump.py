import os

# 🛡️ 1. LOS ESCUDOS (Exclusiones de carpetas y archivos basura)
DIRECTORIOS_IGNORADOS = {
    '.git', '.github', 'env', 'venv', '.env', 'venv_linux', 
    'node_modules', 'target', '__pycache__', 'dist', 'build'
}

EXTENSIONES_IGNORADAS = {
    '.zip', '.tar', '.gz', '.exe', '.dll', '.so', '.dylib', 
    '.png', '.jpg', '.jpeg', '.gif', '.ico', '.pdf', '.pyc',
    '.sqlite3', '.db', '.log', '.mp4'
}

ARCHIVOS_IGNORADOS = {
    '.env', '.env.local', '.env.development', '.env.production',
    'package-lock.json', 'Cargo.lock', 'yarn.lock'
}

def generar_dump_codigo(directorio_raiz='.', archivo_salida='codigo_completo_dump.txt'):
    print(f"🕵️‍♂️ Escaneando directorio: {os.path.abspath(directorio_raiz)}")
    archivos_procesados = 0
    
    with open(archivo_salida, 'w', encoding='utf-8') as f_out:
        for raiz, directorios, archivos in os.walk(directorio_raiz):
            
            # ✂️ Poda el árbol en tiempo real: Si ve una carpeta ignorada, ni siquiera entra a leerla
            directorios[:] = [d for d in directorios if d not in DIRECTORIOS_IGNORADOS]
            
            for archivo in archivos:
                ext = os.path.splitext(archivo)[1].lower()
                
                # Filtramos los archivos que no le sirven a la IA
                if archivo in ARCHIVOS_IGNORADOS or ext in EXTENSIONES_IGNORADAS or archivo.startswith('.env'):
                    continue
                
                ruta_completa = os.path.join(raiz, archivo)
                ruta_relativa = os.path.relpath(ruta_completa, directorio_raiz)
                
                # Normalizamos las barras para que siempre sean diagonales (/) sin importar si estás en Windows o Linux
                ruta_relativa = ruta_relativa.replace('\\', '/')
                
                # Evitar que el script lea a sí mismo o al archivo de salida
                if ruta_relativa == archivo_salida or ruta_relativa == os.path.basename(__file__):
                    continue

                try:
                    # Leemos el código y lo escribimos en el formato exacto que pediste
                    with open(ruta_completa, 'r', encoding='utf-8') as f_in:
                        contenido = f_in.read()
                        
                        f_out.write(f"[[{ruta_relativa}]]\n")
                        f_out.write(contenido)
                        f_out.write("\n\n")
                        
                        archivos_procesados += 1
                        
                except UnicodeDecodeError:
                    # Si por accidente intenta leer un archivo binario o imagen sin extensión, lo salta sin crashear
                    print(f"⚠️ Saltando archivo binario no reconocido: {ruta_relativa}")
                    continue

    print(f"✅ ¡Dump completado! Se empaquetaron {archivos_procesados} archivos en '{archivo_salida}'.")

if __name__ == "__main__":
    # Ejecuta la función en el directorio actual
    generar_dump_codigo()
