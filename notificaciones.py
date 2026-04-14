import json
import urllib.request
from config import PATHS
from utils import console, logger

def enviar_notificacion_bot(mensaje, plataforma="telegram"):
    """
    Envía una notificación al finalizar un proceso a un bot.
    Requiere definir los tokens en tu archivo config.py (PATHS).
    """
    try:
        if plataforma == "telegram":
            # Reemplaza con tus credenciales reales en config.py
            bot_token = PATHS.get("telegram_bot_token", "TU_TOKEN_AQUI") 
            chat_id = PATHS.get("telegram_chat_id", "TU_CHAT_ID_AQUI")
            
            if bot_token == "TU_TOKEN_AQUI":
                console.print("[dim]⚠️ Bot de Telegram no configurado. Omitiendo notificación.[/]")
                return
                
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = json.dumps({"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}).encode('utf-8')
            headers = {'Content-Type': 'application/json'}
            
        elif plataforma == "webhook": # Para Slack / Discord / Teams
            webhook_url = PATHS.get("webhook_url", "TU_WEBHOOK_AQUI")
            if webhook_url == "TU_WEBHOOK_AQUI":
                return
            url = webhook_url
            # Formato estándar de payload para Discord/Slack
            data = json.dumps({"content": mensaje, "text": mensaje}).encode('utf-8')
            headers = {'Content-Type': 'application/json'}
            
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status in [200, 204]:
                console.print("[bold green]📲 Notificación enviada al Bot exitosamente.[/]")
            else:
                console.print(f"[bold red]❌ Error enviando notificación. Código: {response.status}[/]")
                
    except Exception as e:
        logger.error(f"Fallo al enviar notificación bot: {e}")
        console.print(f"[dim red]⚠️ No se pudo notificar al bot: {e}[/]")