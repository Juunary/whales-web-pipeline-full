async function loadJSON(path) {
  const r = await fetch(path + "?_=" + Date.now());
  return await r.json();
}
async function loadCSV(path) {
  const r = await fetch(path + "?_=" + Date.now());
  return await r.text();
}
function donut(el, title, data) {
  const labels = Object.keys(data); const values = labels.map(k => data[k]);
  Plotly.newPlot(el, [{type:'pie', labels, values, hole:0.55}], {height:300, title}, {displayModeBar:false});
}
(async function(){
  const avg = await loadJSON('data/averages.json');
  document.getElementById('asof').textContent = 'as of ' + new Date(avg.timestamp_utc*1000).toISOString() + ' — wallets: ' + avg.wallet_count;
  donut('avg-ew', 'Equal-Weight', avg.averages_T.equal_weight);
  donut('avg-vw', 'Value-Weight', avg.averages_T.value_weight);

  const priceCSV = await loadCSV('data/token_24h_change.csv');
  const lines = priceCSV.trim().split(/\r?\n/).slice(1);
  const tbody = document.querySelector('#price-table tbody'); tbody.innerHTML = '';
  lines.slice(0, 30).forEach(l => { const [c, p] = l.split(','); const tr = document.createElement('tr'); const td1=document.createElement('td'); td1.textContent=c; const td2=document.createElement('td'); td2.textContent=parseFloat(p).toFixed(2)+'%'; tr.appendChild(td1); tr.appendChild(td2); tbody.appendChild(tr); });

  const sText = await loadCSV('data/samples_top10.csv');
  const sRows = sText.trim().split(/\r?\n/).map(l => l.split(','));
  const headers = sRows[0]; const body = sRows.slice(1);
  const table = document.createElement('table');
  const thead = document.createElement('thead'); const trh = document.createElement('tr');
  headers.forEach(h => { const th=document.createElement('th'); th.textContent=h; trh.appendChild(th);}); thead.appendChild(trh);
  const tb = document.createElement('tbody');
  body.forEach(r => { const tr=document.createElement('tr'); r.forEach(c => { const td=document.createElement('td'); td.textContent=c; tr.appendChild(td);}); tb.appendChild(tr);});
  table.appendChild(thead); table.appendChild(tb);
  document.getElementById('sample-table').appendChild(table);
})();