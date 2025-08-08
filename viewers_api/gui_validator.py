# gui_validator.py
import asyncio
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import List

import aiohttp

MAX_CONCURRENCY = 100
DEFAULT_PROXIES_PATH = "proxies.txt"
DEFAULT_TOKENS_PATH  = "tokens.txt"

def parse_proxy_line(line: str) -> str | None:
    """
    Поддержка формата host:port[:user:pass]
    Возвращает прокси-URL: http://user:pass@host:port или http://host:port
    """
    line = line.strip()
    if not line:
        return None
    parts = line.split(":")
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    return None

async def validate_proxy(proxy_url: str, session: aiohttp.ClientSession) -> bool:
    """
    Быстрый пинг через прокси к 204-эндпоинту.
    204 -> ок. Любой сетевой эксепшен -> не ок.
    """
    try:
        async with session.get(
                "https://www.google.com/generate_204",
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return r.status in (200, 204)
    except Exception:
        return False

async def validate_token(token: str, session: aiohttp.ClientSession) -> bool:
    """
    Валидируем токен через /oauth2/validate (официальный способ).
    200 -> валидный.
    """
    try:
        async with session.get(
                "https://id.twitch.tv/oauth2/validate",
                headers={"Authorization": f"OAuth {token.strip()}"},
                timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            return r.status == 200
    except Exception:
        return False

async def run_validations(
        proxies_raw: List[str], tokens_raw: List[str],
        progress_cb=None
) -> tuple[list[str], list[str]]:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    valid_proxies: list[str] = []
    valid_tokens:  list[str] = []

    async with aiohttp.ClientSession() as sess:
        # Прокси
        proxy_urls = [p for p in (parse_proxy_line(x) for x in proxies_raw) if p]

        async def _vp(purl: str):
            async with sem:
                ok = await validate_proxy(purl, sess)
                if ok:
                    valid_proxies.append(purl)
                if progress_cb:
                    progress_cb()

        # Токены
        async def _vt(tok: str):
            async with sem:
                ok = await validate_token(tok, sess)
                if ok:
                    valid_tokens.append(tok.strip())
                if progress_cb:
                    progress_cb()

        tasks: list[asyncio.Task] = []
        for p in proxy_urls:
            tasks.append(asyncio.create_task(_vp(p)))
        for t in tokens_raw:
            if t.strip():
                tasks.append(asyncio.create_task(_vt(t)))

        if tasks:
            await asyncio.gather(*tasks)

    return valid_proxies, valid_tokens

class ValidateWindow(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.title("Проверка прокси / токенов")
        self.geometry("720x520")
        self.resizable(True, True)

        # источники
        src_frame = tk.Frame(self); src_frame.pack(fill=tk.X, padx=10, pady=8)

        self.use_files_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            src_frame, text="Брать из proxies.txt / tokens.txt",
            variable=self.use_files_var
        ).pack(side=tk.LEFT)

        tk.Button(src_frame, text="Выбрать proxies.txt", command=self._pick_proxies).pack(side=tk.LEFT, padx=6)
        tk.Button(src_frame, text="Выбрать tokens.txt",  command=self._pick_tokens).pack(side=tk.LEFT, padx=6)

        self.proxies_path = tk.StringVar(value=DEFAULT_PROXIES_PATH)
        self.tokens_path  = tk.StringVar(value=DEFAULT_TOKENS_PATH)

        # текстовые поля (если не из файлов)
        paned = tk.PanedWindow(self, orient=tk.HORIZONTAL); paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)

        left = tk.Frame(paned); right = tk.Frame(paned)
        paned.add(left); paned.add(right)

        tk.Label(left, text="Прокси (host:port[:user:pass], по одному в строке):").pack(anchor="w")
        self.txt_proxies = tk.Text(left, height=15)
        self.txt_proxies.pack(fill=tk.BOTH, expand=True)

        tk.Label(right, text="Токены (по одному в строке):").pack(anchor="w")
        self.txt_tokens = tk.Text(right, height=15)
        self.txt_tokens.pack(fill=tk.BOTH, expand=True)

        # прогресс
        prog_fr = tk.Frame(self); prog_fr.pack(fill=tk.X, padx=10, pady=6)
        self.progress = ttk.Progressbar(prog_fr, length=420, mode="determinate")
        self.progress.pack(side=tk.LEFT)
        self.lbl_progress = tk.Label(prog_fr, text="0/0")
        self.lbl_progress.pack(side=tk.LEFT, padx=8)

        # кнопки
        btn_fr = tk.Frame(self); btn_fr.pack(fill=tk.X, padx=10, pady=8)
        tk.Button(btn_fr, text="Старт проверок", command=self._start).pack(side=tk.LEFT)
        tk.Button(btn_fr, text="Закрыть", command=self.destroy).pack(side=tk.RIGHT)

        # результаты
        res_fr = tk.Frame(self); res_fr.pack(fill=tk.X, padx=10, pady=6)
        self.lbl_res = tk.Label(res_fr, text="Ожидание запуска…")
        self.lbl_res.pack(anchor="w")

    def _pick_proxies(self):
        path = filedialog.askopenfilename(title="proxies.txt", filetypes=[("Text", "*.txt"), ("All", "*.*")])
        if path:
            self.proxies_path.set(path)

    def _pick_tokens(self):
        path = filedialog.askopenfilename(title="tokens.txt", filetypes=[("Text", "*.txt"), ("All", "*.*")])
        if path:
            self.tokens_path.set(path)

    def _load_sources(self) -> tuple[list[str], list[str]]:
        if self.use_files_var.get():
            try:
                with open(self.proxies_path.get(), "r", encoding="utf-8") as f:
                    proxies = [l.strip() for l in f if l.strip()]
            except FileNotFoundError:
                proxies = []
            try:
                with open(self.tokens_path.get(), "r", encoding="utf-8") as f:
                    tokens = [l.strip() for l in f if l.strip()]
            except FileNotFoundError:
                tokens = []
        else:
            proxies = [l.strip() for l in self.txt_proxies.get("1.0", "end").splitlines() if l.strip()]
            tokens  = [l.strip() for l in self.txt_tokens.get("1.0", "end").splitlines() if l.strip()]
        return proxies, tokens

    def _save_results(self, proxies: list[str], tokens: list[str]):
        # сохраняем в стандартные файлы (их потом использует сервер/GUI)
        with open(DEFAULT_PROXIES_PATH, "w", encoding="utf-8") as f:
            # сохраняем в исходном формате host:port[:user:pass], а не URL
            for purl in proxies:
                # обратно к строке без схемы для совместимости (если нужно — можно хранить URL)
                if "@" in purl:
                    creds, hostpart = purl.split("@", 1)
                    scheme, creds = creds.split("://", 1)
                    host, port = hostpart.split(":")
                    user, pwd = creds.split(":")
                    line = f"{host}:{port}:{user}:{pwd}"
                else:
                    _, hostpart = purl.split("://", 1)
                    line = hostpart
                f.write(line + "\n")

        with open(DEFAULT_TOKENS_PATH, "w", encoding="utf-8") as f:
            for t in tokens:
                f.write(t + "\n")

    def _start(self):
        proxies, tokens = self._load_sources()
        total = len([p for p in proxies if p.strip()]) + len([t for t in tokens if t.strip()])
        if total == 0:
            messagebox.showwarning("Пусто", "Нет данных для проверки.")
            return

        self.progress.configure(maximum=total, value=0)
        self.lbl_progress.configure(text=f"0/{total}")
        self.lbl_res.configure(text="Валидация запущена… Это может занять время.")

        def bump():
            v = self.progress["value"] + 1
            self.progress["value"] = v
            self.lbl_progress.configure(text=f"{int(v)}/{total}")
            self.update_idletasks()

        async def run():
            vp, vt = await run_validations(proxies, tokens, progress_cb=bump)
            self._save_results(vp, vt)
            self.lbl_res.configure(text=f"Готово. Валидные прокси: {len(vp)} | Валидные токены: {len(vt)}")
            messagebox.showinfo("Готово", f"Прокси OK: {len(vp)}\nТокены OK: {len(vt)}\nСохранено в {DEFAULT_PROXIES_PATH} и {DEFAULT_TOKENS_PATH}")

        # гоняем асинхронку в отдельном треде, чтобы не замораживать UI
        import threading
        def _bg():
            asyncio.run(run())
        threading.Thread(target=_bg, daemon=True).start()
