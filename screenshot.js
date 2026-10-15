const puppeteer = require('puppeteer');

(async () => {
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();

  // Create a simple local HTML file to load the UI
  const fs = require('fs');
  const path = require('path');

  // Screenshot Admin UI
  await page.setViewport({ width: 1400, height: 900 });
  const adminUrl = 'file://' + path.join(__dirname, 'public', 'admin.html').replace(/\\/g, '/');
  await page.goto(adminUrl, { waitUntil: 'networkidle0' });
  await page.screenshot({ path: 'admin-redesign.jpg', type: 'jpeg', quality: 90, fullPage: true });

  // Screenshot Worker UI
  await page.setViewport({ width: 1400, height: 900 });
  const workerUrl = 'file://' + path.join(__dirname, 'public', 'worker.html').replace(/\\/g, '/');
  await page.goto(workerUrl, { waitUntil: 'networkidle0' });
  await page.screenshot({ path: 'worker-redesign-desktop.jpg', type: 'jpeg', quality: 90, fullPage: true });

  // Screenshot Worker UI (Mobile)
  await page.setViewport({ width: 375, height: 812, isMobile: true });
  await page.goto(workerUrl, { waitUntil: 'networkidle0' });
  await page.screenshot({ path: 'worker-redesign-mobile.jpg', type: 'jpeg', quality: 90, fullPage: true });

  await browser.close();
})();