const axios = require('axios');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Função para mascarar dados sensíveis
function maskSensitiveData(data, maxLength = 8) {
    if (!data || typeof data !== 'string') return '[MASKED]';
    if (data.length <= maxLength) return '[MASKED]';
    return data.substring(0, 4) + '*'.repeat(data.length - 8) + data.substring(data.length - 4);
}

// Função para registrar eventos sem expor dados sensíveis
function secureLog(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logLevel = isError ? 'ERROR' : 'INFO';
    console.log(`[${timestamp}] [${logLevel}] ${message}`);
}

// Função para impedir Spreadsheet Formula Injection
function sanitize(val) {
    if (typeof val !== 'string') return val;
    const formulaChars = ['=', '+', '-', '@'];
    if (formulaChars.some(char => val.startsWith(char))) {
        return `'${val}`;
    }
    return val;
}

function parseDataBR(texto) {
    if (!texto) return null;
    try {
        const limpo = texto.replace(',', '').trim().split(' ')[0];
        const [d, m, y] = limpo.split('/');
        if (!d || !m || !y) return null;
        const dataISO = `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}T00:00:00Z`;
        const dObj = new Date(dataISO);
        return isNaN(dObj.getTime()) ? null : dObj;
    } catch (e) { return null; }
}

function formatarDataBR(dataISO) {
    if (!dataISO) return "";
    try {
        return new Date(new Date(dataISO).getTime() - (3 * 3600000))
            .toLocaleString('pt-BR', { timeZone: 'UTC' })
            .replace(',', '');
    } catch (e) { return ""; }
}

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

    try {
        // --- ETAPA 1: METADADOS ---
        secureLog("Iniciando Etapa 1: Validando Planilha Principal...");
        let meta;
        try {
            meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
            secureLog("Sucesso na Etapa 1.");
        } catch (err1) {
            const code = err1.response?.status || "Rede";
            const msg = err1.response?.data?.error?.message || err1.message;
            secureLog(`ERRO CRÍTICO ETAPA 1 [${code}]: ${msg}`, true);
            throw new Error(`Interrupção na Etapa 1: ${code}`);
        }

        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        // --- ETAPA 2: COLABORADORES ---
        secureLog("Iniciando Etapa 2: Acessando DB Colaboradores...");
        let resColab;
        try {
            // Tentando acessar a planilha de colaboradores
            resColab = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { 
                headers: { 'Authorization': `Bearer ${GOOGLE_TOKEN}` } 
            });
            secureLog("Sucesso na Etapa 2.");
        } catch (err2) {
            const code = err2.response?.status || "Rede";
            const msg = err2.response?.data?.error?.message || err2.message;
            
            // Aqui está o log que vai matar a charada:
            secureLog(`ERRO DETALHADO ETAPA 2 [${code}]: ${msg}`, true);
            
            if (code === 401) {
                secureLog("Pista: 401 aqui significa que o Token é aceito pelo Google, mas não tem permissão neste arquivo específico.");
            } else if (code === 404) {
                secureLog("Pista: 404 significa que o ID da planilha de colaboradores ou o nome da aba está incorreto.");
            }
            throw new Error(`Interrupção na Etapa 2: ${code}`);
        }

        const mapaNomes = {};
        if (resColab.data?.values) {
            resColab.data.values.forEach(r => { if (r[12]) mapaNomes[r[12]] = r[0]; });
        }

        // --- ETAPA 3: LOGIN HABLLA ---
        secureLog("Iniciando Etapa 3: Login na API Hablla...");
        let login;
        try {
            login = await axios.post('https://api.hablla.com/v1/authentication/login', { 
                email: HABLLA_EMAIL, 
                password: HABLLA_PASSWORD 
            });
            secureLog("Sucesso no Login Hablla.");
        } catch (err3) {
            const code = err3.response?.status || "Rede";
            const msg = JSON.stringify(err3.response?.data) || err3.message;
            secureLog(`ERRO CRÍTICO NO LOGIN HABLLA [${code}]: ${msg}`, true);
            throw new Error(`Falha de Autenticação Hablla: ${code}`);
        }
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- ETAPA 4: LIMPEZA COM CRITÉRIO DE PARADA ---
        secureLog("Analisando registros para limpeza (7 dias)...");
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        
        if (resSheet.data?.values) {
            const rows = resSheet.data.values;
            let blocosParaDeletar = [], startIdx = -1, contadorConsecutivasFora = 0;

            for (let i = rows.length - 1; i >= 1; i--) {
                const dataRow = parseDataBR(rows[i][1]);
                if (dataRow && dataRow >= seteDiasAtras) {
                    if (startIdx === -1) startIdx = i;
                    contadorConsecutivasFora = 0;
                } else {
                    contadorConsecutivasFora++;
                    if (startIdx !== -1) {
                        blocosParaDeletar.push({ start: i + 1, end: startIdx + 1 });
                        startIdx = -1;
                    }
                    if (contadorConsecutivasFora >= 20) break;
                }
            }
            if (startIdx !== -1) blocosParaDeletar.push({ start: 1, end: startIdx + 1 });

            if (blocosParaDeletar.length > 0) {
                const requests = blocosParaDeletar.map(b => ({
                    deleteDimension: { range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } }
                }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
            }
        }

        // --- ETAPA 5: BUSCA E INSERÇÃO ---
        secureLog("Sincronizando novos dados da API...");
        let page = 1, paginasSemNovos = 0;

        while (page <= 500) {
            const resApi = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, page: page, updated_after: seteDiasAtras.toISOString() },
                headers: hHeaders
            });

            const cards = resApi.data.results || [];
            if (cards.length === 0) break;

            const rowsToInsert = cards
                .filter(c => new Date(c.created_at) >= seteDiasAtras)
                .map(card => {
                    let cf = ["", "", "", ""];
                    const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                    (card.custom_fields || []).forEach(f => {
                        const idx = ids.indexOf(f.custom_field);
                        if (idx !== -1) cf[idx] = f.value;
                    });
                    const uid = card.user || "";
                    return [
                        formatarDataBR(card.updated_at), formatarDataBR(card.created_at), card.workspace, card.board, card.list,
                        sanitize(cf[0]), sanitize(cf[1]), sanitize(cf[2]), sanitize(card.name), sanitize(card.description), card.source, card.status,
                        uid, formatarDataBR(card.finished_at), card.id, mapaNomes[uid] || "", sanitize(cf[3]), (card.tags || []).map(t => t.name).join(", ")
                    ];
                });

            if (rowsToInsert.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                    { values: rowsToInsert }, { headers: gHeaders });
                await sleep(1500);
                paginasSemNovos = 0;
            } else {
                paginasSemNovos++;
            }
            if (paginasSemNovos >= 2) break;
            page++;
        }

        // --- ETAPA 6: FAXINA DE DUPLICADOS (COLUNA O / ÍNDICE 14) ---
        secureLog("Removendo duplicados da Coluna O...");
        const resF = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R`, { headers: gHeaders });
        if (resF.data?.values) {
            const rows = resF.data.values;
            const mapUnicos = new Map();
            // Preserva o cabeçalho e filtra duplicados pelo ID do Card (Coluna O - Índice 14)
            rows.slice(1).forEach(linha => {
                const cardId = linha[14];
                if (cardId) mapUnicos.set(cardId, linha);
            });
            const dadosFinais = [rows[0], ...mapUnicos.values()];

            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R:clear`, {}, { headers: gHeaders });
            await axios.put(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A1`, 
                { values: dadosFinais }, { params: { valueInputOption: 'USER_ENTERED' }, headers: gHeaders });
            secureLog("Faxina concluída.");
        }

        // --- ETAPA 7: RELATÓRIO DE ATENDENTES ---
        secureLog("Processando Base Atendente...");
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { 
                start_date: new Date(ontem.setHours(0,0,0,0)).toISOString(), 
                end_date: new Date(ontem.setHours(23,59,59,999)).toISOString() 
            },
            headers: hHeaders
        });
        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ 
                ontem.toLocaleDateString('pt-BR'), HABLLA_WORKSPACE_ID, s.id, sanitize(s.name), u.id, mapaNomes[u.id] || "", sanitize(u.email), 
                item.total_services, item.tme, item.tma, c.id, sanitize(c.name), c.type, 
                item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr 
            ];
        });
        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, { values: rowsAt }, { headers: gHeaders });
        }

        secureLog("Sincronização e faxina concluídas.");

    } catch (e) {
        // Tratamento seguro de erro para logs de CI/CD
        const status = e.response ? e.response.status : 'Erro de Rede';
        const data = e.response ? JSON.stringify(e.response.data) : e.message;
        secureLog(`Erro no processo: ${status}`, true);
        process.exit(1);
    }
}

run();
