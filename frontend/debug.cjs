const puppeteer = require('puppeteer');

(async () => {
  console.log('Starting puppeteer...');
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  
  page.on('console', msg => {
    console.log(`PAGE LOG [${msg.type()}]:`, msg.text());
  });
  
  page.on('pageerror', err => {
    console.log('PAGE ERROR:', err.toString());
  });
  
  page.on('requestfailed', request => {
    console.log(`REQUEST FAILED: ${request.url()} - ${request.failure()?.errorText}`);
  });

  try {
    console.log('Navigating to http://localhost:5174');
    await page.goto('http://localhost:5174', { waitUntil: 'networkidle2' });
    console.log('Navigation complete.');
  } catch (err) {
    console.error('Error navigating:', err);
  } finally {
    await browser.close();
  }
})();
