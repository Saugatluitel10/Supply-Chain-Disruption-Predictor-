import { test, expect } from '@playwright/test';

// Basic smoke covering main shell loads and core UI elements render
// Set BASE_URL via env or playwright.config.ts default (http://localhost:5000)

test.describe('Dashboard smoke', () => {
  test.beforeEach(async ({ page }) => {
    // Stub backend API requests with minimal, representative payloads
    await page.route('**/api/**', async (route) => {
      const url = route.request().url();
      if (url.includes('/api/dashboard/overview')) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ totalSuppliers: 42, highRiskVendors: 3, avgLeadTimeDays: 12 })
        });
      }
      if (url.includes('/api/events/recent')) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([
            { id: 'e1', region: 'North America', industry: 'Manufacturing', timestamp: new Date().toISOString(), severity: 'medium', description: 'Port delay' }
          ])
        });
      }
      if (url.includes('/api/risks/recent')) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([
            { id: 'r1', vendor: 'Vendor A', score: 0.76, region: 'Europe', industry: 'Retail', updatedAt: new Date().toISOString() }
          ])
        });
      }
      if (url.includes('/api/alerts/active')) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([
            { id: 'a1', title: 'Factory fire', severity: 'high', createdAt: new Date().toISOString() }
          ])
        });
      }
      // Default: 204 No Content for any other API calls
      return route.fulfill({ status: 204, body: '' });
    });
  });

  test('loads homepage and shows header and filters', async ({ page }) => {
    await page.goto('/');

    // App header title (default language EN)
    await expect(page.getByRole('banner')).toBeVisible();
    await expect(page.getByRole('heading', { name: /supply chain risk dashboard/i })).toBeVisible();

    // Language switcher should exist
    await expect(page.getByRole('combobox')).toBeVisible();

    // Filters for region/industry should exist (translated labels fallback to EN)
    await expect(page.getByLabel(/region/i)).toBeVisible();
    await expect(page.getByLabel(/industry/i)).toBeVisible();

    // Key sections render
    await expect(page.getByText(/risk heat map/i)).toBeVisible();
    await expect(page.getByText(/timeline/i)).toBeVisible();
    await expect(page.getByText(/alerts/i)).toBeVisible();
  });
});
