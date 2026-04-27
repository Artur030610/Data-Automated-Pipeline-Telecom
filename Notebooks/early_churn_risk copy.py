import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# --- CONFIGURACIÓN ESTÉTICA (MODO OSCURO) ---
plt.style.use('dark_background') 
# Colores personalizados (paleta tipo neón)
color_hist = '#1e37aa'  # Azul oscuro para las barras (con alpha)
color_edge = '#4dffff'  # Cian para los bordes
color_lines = '#ff2277' # Magenta vibrante para el IC
color_text = 'lime'     # Lima para el título

# 1. Datos iniciales (Ej: Tasa de Early Churn en Fibex)
np.random.seed(42)
datos_originales = np.random.normal(loc=12, scale=2, size=100)
estadisticos_bootstrap = []

fig, ax = plt.subplots(figsize=(10, 6))

# Configuración inicial del gráfico
ax.set_facecolor('#050505') # Fondo ligeramente más oscuro que el estándar
ax.set_xlim(11, 13)
ax.set_ylim(0, 150) # Ajustado para más iteraciones
ax.grid(color='#222222', linestyle='-', linewidth=0.5) # Rejilla sutil
ax.tick_params(colors='white') # Números de los ejes en blanco

def update(frame):
    # En cada frame añadimos 25 muestras para que avance más rápido
    for _ in range(25):
        muestra = np.random.choice(datos_originales, size=len(datos_originales), replace=True)
        estadisticos_bootstrap.append(np.mean(muestra))
    
    R_actual = len(estadisticos_bootstrap)
    ax.clear()
    
    # RE-CONFIGURAR LOS EJES (porque clear() borra todo)
    ax.set_xlim(11, 13)
    ax.set_ylim(0, 150)
    ax.grid(color='#222222', linestyle='-', linewidth=0.5)
    
    # Título dinámico
    ax.set_title(f"Construyendo Distribución Bootstrap (R = {R_actual} iteraciones)", 
                 color=color_text, fontsize=14, fontweight='bold')
    ax.set_xlabel("Tasa de Churn Estimada (%)", color='white', fontsize=12)
    ax.set_ylabel("Frecuencia (Cantidad de Iteraciones)", color='white', fontsize=12)
    
    # Histograma (con estilo dark)
    ax.hist(estadisticos_bootstrap, bins=40, color=color_hist, edgecolor=color_edge, 
            alpha=0.7, linewidth=0.8)
    
    # Líneas de Intervalo de Confianza y Leyenda
    if R_actual > 100:
        inf, sup = np.percentile(estadisticos_bootstrap, [2.5, 97.5])
        # Dibujar líneas rojas verticales
        ax.axvline(inf, color=color_lines, linestyle='--', linewidth=2.5)
        ax.axvline(sup, color=color_lines, linestyle='--', linewidth=2.5)
        
        # Poner texto del valor encima de la línea
        trans = ax.get_xaxis_transform() # Coordenadas mixtas para el texto
        ax.text(inf - 0.05, 140, f'IC Inf: {inf:.2f}%', color=color_lines, fontsize=10, 
                rotation=90, fontweight='bold')
        ax.text(sup + 0.01, 140, f'IC Sup: {sup:.2f}%', color=color_lines, fontsize=10, 
                rotation=90, fontweight='bold')

# frames=80 * 25 muestras = 2,000 iteraciones finales
ani = FuncAnimation(fig, update, frames=80, interval=60, repeat=False)
plt.show() # En VS Code local, esto abre la ventana interactiva