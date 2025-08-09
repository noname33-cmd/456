# gui_manager.py
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import sys
import requests
import threading

API_URL = "http://127.0.0.1:7702/api"


def _base():
    return API_URL if API_URL.endswith("/") else API_URL + "/"


class ViewerGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Twitch Viewer Manager (Python)")
        self.geometry("820x520")
        self.resizable(False, False)

        # Таблица задач
        self.tree = ttk.Treeview(self, columns=("id", "channel", "status", "viewers"), show="headings")
        self.tree.heading("id", text="Task ID")
        self.tree.heading("channel", text="Channel")
        self.tree.heading("status", text="Status")
        self.tree.heading("viewers", text="Viewers")
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Кнопки действий
        btn_frame = tk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)

        tk.Button(btn_frame, text="Обновить", command=self.refresh).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Создать", command=self.create_task).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Стоп", command=lambda: self.control_task("stop")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Пауза", command=lambda: self.control_task("pause")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Продолжить", command=lambda: self.control_task("resume")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Рейд", command=self.raid_task).pack(side=tk.LEFT, padx=5)

        # Справа — утилитки
        tk.Button(btn_frame, text="Проверить Kasada", command=self.check_kasada).pack(side=tk.RIGHT, padx=5)
        tk.Button(btn_frame, text="Проверить списки", command=self.open_validator).pack(side=tk.RIGHT, padx=5)

        self.refresh()

    # ==== API ====

    def refresh(self):
        threading.Thread(target=self._refresh_thread, daemon=True).start()

    def _refresh_thread(self):
        try:
            r = requests.get(_base(), timeout=10)

            if r.status_code == 200:
                data = r.json()
                tasks = data.get("tasks", data)
            elif r.status_code == 400:
                tasks = []
            else:
                r.raise_for_status()

            self.tree.delete(*self.tree.get_children())
            for t in tasks:
                viewers = t.get("number_of_viewers", t.get("viewers", 0))
                self.tree.insert(
                    "", tk.END,
                    values=(t.get("id", ""), t.get("channel", ""), t.get("status", ""), viewers)
                )
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))

    # ==== UI: Создание задачи ====

    def create_task(self):
        win = tk.Toplevel(self)
        win.title("Создать задачу")
        win.geometry("360x260")
        win.resizable(False, False)

        def row(label, default=""):
            frm = tk.Frame(win); frm.pack(fill=tk.X, pady=6)
            tk.Label(frm, text=label, width=18, anchor="w").pack(side=tk.LEFT)
            ent = tk.Entry(frm); ent.pack(side=tk.RIGHT, fill=tk.X, expand=True)
            if default != "":
                ent.insert(0, default)
            return ent

        channel = row("Канал:")
        viewers = row("Зрителей:", "20")
        time_minutes = row("Время (мин):", "10")
        percent_auth = row("% авторизованных:", "0")

        def send_create():
            try:
                n_viewers = int(viewers.get())
                t_minutes = max(1, int(time_minutes.get() or 1))
                p_auth = max(0, int(percent_auth.get() or 0))

                payload = {
                    "channel": channel.get().strip(),
                    "number_of_viewers": n_viewers,
                    "percent_auth_viewers": p_auth,
                    "time_in_minutes": t_minutes,
                    "floating_online": {
                        "enable": False,
                        "percent_min_viewers": 0,
                        "percent_max_viewers": 0,
                        "percent": 0,
                        "delay": 60,
                    },
                    "raid": {
                        "enable": False,
                        "depth": 0,
                        "percent_dropping_in_minute": 0,
                        "percent": 100,
                        "dropping_raid": False,
                    },
                }

                r = requests.post(_base(), json=payload, timeout=120)
                r.raise_for_status()

                try:
                    data = r.json()
                except Exception:
                    data = {}

                if "task" in data and data["task"]:
                    msg = f"Task создана: {data['task'].get('id')}"
                elif "task_id" in data:
                    msg = f"Task создана: {data['task_id']}"
                else:
                    msg = "Task создана"

                messagebox.showinfo("OK", msg)
                self.refresh()
                win.destroy()
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))

        tk.Button(win, text="Создать", command=send_create).pack(pady=8)

    # ==== Действия над задачей ====

    def control_task(self, action):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Выбор", "Выберите задачу")
            return
        task_id = self.tree.item(selected[0])["values"][0]
        try:
            if action == "stop":
                r = requests.delete(f"{_base()}{task_id}", timeout=10)
            elif action == "pause":
                r = requests.post(f"{_base()}{task_id}/pause", timeout=10)
            elif action == "resume":
                r = requests.post(f"{_base()}{task_id}/resume", timeout=10)
            else:
                messagebox.showwarning("Действие", f"Неизвестное действие: {action}")
                return

            if r.status_code >= 400:
                messagebox.showerror("Ошибка", r.text)
            else:
                self.refresh()
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))

    # ==== Рейд ====

    def raid_task(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Выбор", "Выберите задачу")
            return
        # FIX: без лишних скобок
        task_id = self.tree.item(selected[0])["values"][0]

        win = tk.Toplevel(self)
        win.title("Рейд")
        win.geometry("250x150")
        win.resizable(False, False)

        tk.Label(win, text="Целевой канал:").pack()
        target = tk.Entry(win); target.pack()
        tk.Label(win, text="Задержка (сек):").pack()
        delay = tk.Entry(win); delay.insert(0, "5"); delay.pack()

        def send_raid():
            try:
                data = {"target_channel": target.get(), "delay": int(delay.get())}
                r = requests.post(f"{_base()}{task_id}/raid", json=data, timeout=10)
                if r.status_code >= 400:
                    messagebox.showerror("Ошибка", r.text)
                else:
                    messagebox.showinfo("OK", f"Рейд запущен на {target.get()}")
                    win.destroy()
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))

        tk.Button(win, text="Запустить", command=send_raid).pack(pady=10)

    # ==== Kasada ====

    def check_kasada(self):
        threading.Thread(target=self._check_kasada_thread, daemon=True).start()

    def _check_kasada_thread(self):
        try:
            r = requests.get(_base() + "kasada/check", timeout=15)
            r.raise_for_status()
            data = r.json()
            self._show_kasada_result(data)
        except Exception as e:
            messagebox.showerror("Kasada", f"Ошибка проверки: {e}")

    def _show_kasada_result(self, data: dict):
        res = data.get("results", {})
        cached = data.get("cached")
        cds = data.get("cooldowns", {})

        def _line(name: str):
            item = res.get(name, {})
            if not item:
                return f"{name}: —"
            if item.get("enabled") is False:
                return f"{name}: выключен"
            ok = item.get("ok")
            if ok:
                ms = item.get("ms", 0)
                return f"{name}: OK ({ms} ms)"
            err = item.get("error", "unknown error")
            return f"{name}: FAIL — {err}"

        lines = [
            f"Кэш провайдера: {cached or '—'}",
            f"Кулдаун: notion={cds.get('notion',0)}s, salamoonder={cds.get('salamoonder',0)}s",
            "",
            _line("notion"),
            _line("salamoonder"),
            _line("fallback"),
        ]
        messagebox.showinfo("Kasada", "\n".join(lines))

    # ==== Открыть отдельный валидатор списков ====

    def open_validator(self):
        """
        Открывает отдельное окно валидатора списков (proxies/tokens).
        Ожидается, что есть gui_validator.py в корне проекта.
        """
        try:
            subprocess.Popen([sys.executable, "gui_validator.py"])
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось открыть валидатор: {e}")


if __name__ == "__main__":
    app = ViewerGUI()
    app.mainloop()
