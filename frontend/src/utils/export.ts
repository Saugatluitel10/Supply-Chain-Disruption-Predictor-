import html2canvas from 'html2canvas';

export function exportToCSV(filename: string, rows: Array<Record<string, any>>) {
  if (!rows || rows.length === 0) {
    console.warn('No data to export');
    return;
  }
  const headers = Object.keys(rows[0]);
  const csv = [headers.join(',')]
    .concat(
      rows.map((row) =>
        headers
          .map((h) => {
            let v = row[h];
            if (v === null || v === undefined) v = '';
            // escape quotes and commas
            const s = String(v).replace(/"/g, '""');
            return /[",\n]/.test(s) ? `"${s}"` : s;
          })
          .join(',')
      )
    )
    .join('\n');

  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename.endsWith('.csv') ? filename : `${filename}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export async function exportElementToPNG(elementId: string, filename: string) {
  const el = document.getElementById(elementId);
  if (!el) {
    console.warn(`Element #${elementId} not found`);
    return;
  }
  const canvas = await html2canvas(el, { scale: 2 });
  const dataUrl = canvas.toDataURL('image/png');
  const link = document.createElement('a');
  link.href = dataUrl;
  link.download = filename.endsWith('.png') ? filename : `${filename}.png`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
