/* ==========================================================================
   MLB Dashboard — Theme Management (Dark / Light / Auto)
   ========================================================================== */

import { state } from './state.js';

export function initTheme() {
    const saved = localStorage.getItem('mlb_theme') || 'auto';
    setTheme(saved);
}

export function setTheme(t) {
    state.theme = t;
    localStorage.setItem('mlb_theme', t);
    const html = document.documentElement;
    html.classList.remove('theme-light', 'theme-dark', 'theme-auto');

    const icon = document.getElementById('theme-icon');
    if (t === 'light') {
        html.classList.add('theme-light');
        if (icon) icon.textContent = '☀️';
    } else if (t === 'dark') {
        html.classList.add('theme-dark');
        if (icon) icon.textContent = '🌙';
    } else {
        html.classList.add('theme-auto');
        if (icon) icon.textContent = '🌓';
    }
}

export function cycleTheme() {
    if (state.theme === 'auto') setTheme('dark');
    else if (state.theme === 'dark') setTheme('light');
    else setTheme('auto');
}

export function getEffectiveTheme() {
    if (state.theme === 'light') return 'light';
    if (state.theme === 'dark') return 'dark';
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}
