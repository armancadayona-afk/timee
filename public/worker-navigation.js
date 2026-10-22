(() => {
  const pages = [...document.querySelectorAll('[data-page]')];
  const screens = [...document.querySelectorAll('.screen')];
  function showPage(name) {
    screens.forEach(screen => screen.classList.toggle('active', screen.id === `screen-${name}`));
    pages.forEach(button => button.classList.toggle('active', button.dataset.page === name));
  }
  pages.forEach(button => button.addEventListener('click', () => showPage(button.dataset.page)));
  const settings = document.getElementById('settings-btn');
  if (settings) settings.addEventListener('click', () => showPage('profile'));
  const theme = document.getElementById('theme-toggle');
  if (theme) theme.addEventListener('click', () => {
    document.body.classList.toggle('dark');
    theme.textContent = document.body.classList.contains('dark') ? '☀️ Light' : '🌙 Dark';
  });
})();
