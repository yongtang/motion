import { test, expect } from '@playwright/test';

test('Vite + React homepage renders', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveTitle(/Vite \+ React/i);
  await expect(page.getByRole('heading', { name: /Vite \+ React/i })).toBeVisible();
});
