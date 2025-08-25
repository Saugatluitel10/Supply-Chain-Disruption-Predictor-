{{ ... }}

export default function () {
  const res = http.get(`${BASE_URL}/`);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'served some content': (r) => r.body && r.body.length > 100,
  });
  sleep(1);
}

+export function handleSummary(data) {
+  return {
+    'k6-summary.json': JSON.stringify(data, null, 2),
+  };
+}
