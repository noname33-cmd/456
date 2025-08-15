import "./globals.css";
export const metadata = { title: "Pattern Controller UI", description: "Dashboard for pattern_controller" };
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru">
      <body>
        <div className="container">
          <header style={{display:"flex",justifyContent:"space-between",alignItems:"end",gap:12,margin:"12px 0 20px"}}>
            <div>
              <h1 style={{margin:0}}>Pattern Controller — Dashboard</h1>
              <div className="small">Управление пулами и просмотр отчётов (/report/*)</div>
            </div>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
