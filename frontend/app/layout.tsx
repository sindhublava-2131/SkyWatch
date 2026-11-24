import type { Metadata, Viewport } from 'next'
import { IBM_Plex_Mono, IBM_Plex_Sans } from 'next/font/google'
import './globals.css'
const sans=IBM_Plex_Sans({subsets:['latin'],weight:['400','500','600'],variable:'--font-sans'})
const mono=IBM_Plex_Mono({subsets:['latin'],weight:['400','500'],variable:'--font-mono'})
export const metadata:Metadata={title:'SkyWatch · Bengaluru air quality',description:'Read-only monitoring console for particulate air quality across Bengaluru.'}
export const viewport:Viewport={themeColor:[{media:'(prefers-color-scheme: light)',color:'#F7F6F2'},{media:'(prefers-color-scheme: dark)',color:'#121311'}],colorScheme:'light dark'}
export default function RootLayout({children}:{children:React.ReactNode}){return <html lang="en-IN" suppressHydrationWarning><body className={`${sans.variable} ${mono.variable}`}>{children}</body></html>}
