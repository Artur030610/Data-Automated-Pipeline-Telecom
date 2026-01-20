import pandas as pd
import re

# Tu lista de oficinas
data = ["OFI-PASEO LAS INDUSTRIAS", "OFC - TROMP ELECTRONIC", "OFICINA SAN JUAN DE LOS MORROS", ...] 
df = pd.DataFrame(data, columns=['Nombre_Original'])

def limpiar_nombre(texto):
    if not isinstance(texto, str):
        return texto
    
    # 1. Convertir a mayúsculas y quitar espacios extremos
    texto = texto.upper().strip()
    
    # 2. Eliminar prefijos comunes (OFI, OFIC, OFC, etc.) usando Regex
    # Explicación: Busca al inicio (^) variantes de OFICINA seguidas de guiones o espacios
    patron = r'^(OFICINA|OFIC|OFI|OFC|AGENTE AUTORIZADO|AGET AUTORIZADO)(\s*[-_.]\s*|\s+)'
    texto = re.sub(patron, '', texto)
    
    # 3. Eliminar sufijos o palabras ruidosas si es necesario
    # Ejemplo: Quitar "AGENTE AUTORIZADO" si aparece al final
    texto = texto.replace('AGENTE AUTORIZADO', '').strip()
    
    # 4. Limpieza de caracteres especiales residuales al inicio/fin
    texto = texto.strip(" -_.")
    
    return texto

# Aplicar la limpieza
df['Nombre_Limpio'] = df['Nombre_Original'].apply(limpiar_nombre)

# Eliminar duplicados resultantes para tener tu lista maestra
lista_maestra = df['Nombre_Limpio'].drop_duplicates().sort_values()

print(df.head(10))