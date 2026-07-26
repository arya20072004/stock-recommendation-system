import React from 'react';
import { useTheme } from '../theme/ThemeContext';
import { Sun, Moon } from 'lucide-react';

export const ThemeToggle = () => {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      onClick={toggleTheme}
      className="theme-toggle"
      aria-label="Toggle theme"
      title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
    >
      {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
      <style>{`
        .theme-toggle {
          display: flex;
          align-items: center;
          justify-content: center;
          width: 40px;
          height: 40px;
          border-radius: 50%;
          background-color: var(--bg-hover);
          color: var(--text-primary);
          transition: background-color 0.2s ease, transform 0.2s ease;
        }
        
        .theme-toggle:hover {
          background-color: var(--bg-card);
          transform: scale(1.05);
        }

        @media (prefers-reduced-motion: reduce) {
          .theme-toggle {
            transition: none;
          }
          .theme-toggle:hover {
            transform: none;
          }
        }
      `}</style>
    </button>
  );
};
