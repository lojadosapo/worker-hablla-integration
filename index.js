const axios = require('axios');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- FUNÇÕES DE APOIO ---

function formatarDataBR(dataISO) {
    if (!dataISO) return "";
    return new Date(new Date(dataISO).getTime() - (3 * 3600000))
        .toLocaleString('pt-BR', { timeZone: 'UTC' })
        .replace(',', '');
}

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

    try {
        // --- ETAPA 1: METADADOS (RESOLVE O ERRO DE GID) ---
        console.log(">>> [ETAPA 1] Obtendo IDs das abas na planilha...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
        
        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        // --- ETAPA 2: SINCRONIZAÇÃO DE COLABORADORES ---
        console.log(">>> [ETAPA 2] Mapeando nomes de colaboradores...");
        const resColab = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { headers: gHeaders });
        const mapaNomes = {};
        if (resColab.data?.values) {
            resColab.data.values.forEach(r => { if (r[12]) mapaNomes[r[12]] = r[0]; });
        }

        // --- ETAPA 3: LOGIN HABLLA ---
        console.log(">>> [ETAPA 3] Autenticando na API Hablla...");
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- DEFINIÇÃO DE JANELA DE TEMPO (7 DIAS) ---
        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        // --- ETAPA 4: LEITURA E IDENTIFICAÇÃO DE LIMPEZA ---
        console.log(`>>> [ETAPA 4] Analisando registros de ${seteDiasAtras.toLocaleDateString('pt-BR')} até hoje para apagar...`);
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        
        if (resSheet.data?.values) {
            const rows = resSheet.data.values;
            let blocosParaDeletar = [];
            let startIdx = -1;

            // Percorre de baixo para cima para identificar sequências de datas recentes
            for (let i = rows.length - 1; i >= 1; i--) {
                const dataStr = rows[i][1]; // Coluna B
                if (!dataStr) continue;

                const [data] = dataStr.split(' ');
                const [d, m, y] = data.split('/');
                const dataRow = new Date(`${y}-${m}-${d}T00:00:00Z`);

                if (dataRow >= seteDiasAtras) {
                    if (startIdx === -1) startIdx = i; 
                } else {
                    if (startIdx !== -1) {
                        blocosParaDeletar.push({ start: i + 1, end: startIdx + 1 });
                        startIdx = -1;
                    }
                }
            }
            if (startIdx !== -1) blocosParaDeletar.push({ start: 1, end: startIdx + 1 });

            // --- ETAPA 5: EXCUÇÃO DA LIMPEZA (DELETE DIMENSION) ---
            if (blocosParaDeletar.length > 0) {
                console.log(`>>> [ETAPA 5] Executando limpeza de ${blocosParaDeletar.length} bloco(s)...`);
                const requests = blocosParaDeletar.map(b => ({
                    deleteDimension: { 
                        range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } 
                    }
                }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                console.log("Limpeza concluída.");
            } else {
                console.log(">>> [ETAPA 5] Nenhuma linha encontrada para apagar.");
            }
        }

        // --- ETAPA 6: BUSCA E INSERÇÃO DE NOVOS DADOS ---
        console.log(">>> [ETAPA 6] Buscando dados atualizados na API...");
        let page = 1, totalPages = 1, paginasSemNovos = 0;

        while (page <= totalPages) {
            const resApi = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, page: page, updated_after: seteDiasAtras.toISOString() },
                headers: hHeaders
            });

            const cards = resApi.data.results || [];
            totalPages = resApi.data.totalPages || 1;
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
                    return [
                        formatarDataBR(card.updated_at), formatarDataBR(card.created_at), card.workspace, card.board, card.list,
                        cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                        card.user, formatarDataBR(card.finished_at), card.id, mapaNomes[card.user] || "", cf[3], (card.tags || []).map(t => t.name).join(", ")
                    ];
                });

            if (rowsToInsert.length > 0) {
                console.log(`Inserindo ${rowsToInsert.length} registros da Pág ${page}...`);
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

        // --- ETAPA 7: RELATÓRIO DE ATENDENTES (ONTEM) ---
        console.log(">>> [ETAPA 7] Processando Base Atendente...");
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const dIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
        const dFim = new Date(ontem.setHours(23,59,59,999)).toISOString();

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dIni, end_date: dFim }, headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ ontem.toLocaleDateString('pt-BR'), HABLLA_WORKSPACE_ID, s.id, s.name, u.id, mapaNomes[u.id] || "", u.email, item.total_services, item.tme, item.tma, c.id, c.name, c.type, item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, { values: rowsAt }, { headers: gHeaders });
        }

        console.log(">>> [SUCESSO] Sincronização concluída.");

    } catch (e) {
        console.error("!!! ERRO NO PROCESSO !!!");
        console.error(e.response ? JSON.stringify(e.response.data, null, 2) : e.message);
        process.exit(1);
    }
}

run();
