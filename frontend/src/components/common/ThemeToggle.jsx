import React from 'react';
import { useTheme } from '../../context/ThemeContext';

export const ThemeToggle = () => {
  const { themePreference, setThemePreference } = useTheme();

  return (
    <div className="theme-toggle" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
      <label htmlFor="theme-select" style={{ fontSize: 'var(--font-size-sm)', color: 'var(--color-text-secondary)' }}>
        Theme:
      </label>
      <select
        id="theme-select"
        value={themePreference}
        onChange={(e) => setThemePreference(e.target.value)}
        style={{
          padding: '0.25rem 0.5rem',
          borderRadius: 'var(--radius-sm)',
          border: '1px solid var(--color-border-default)',
          background: 'var(--color-surface-default)',
          color: 'var(--color-text-primary)',
          fontSize: 'var(--font-size-sm)',
          cursor: 'pointer',
        }}
        aria-label="Select theme"
      >
        <option value="light">Light</option>
        <option value="dark">Dark</option>
        <option value="system">System</option>
      </select>
    </div>
  );
};

export default ThemeToggle;
