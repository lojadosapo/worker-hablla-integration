const axios = require('axios');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Função robusta para converter datas da planilha (ex: 19/3/2026 ou 19/03/2026)
function parseDataBR(texto) {
    if (!texto) return null;
    try {
        const limpo = texto.replace(',', '').trim().split(' ')[0];
        const [d, m, y] = limpo.split('/');
        if (!d || !m || !y) return null;
        // Normaliza para o formato ISO YYYY-MM-DD
        const dataISO = `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}T00:00:00Z`;
        const dObj = new Date(dataISO);
        return isNaN(dObj.getTime()) ? null : dObj;
    } catch (e) {
        return null;
    }
}

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
        // --- ETAPA 1: METADADOS ---
        console.log(">>> [ETAPA 1] Obtendo IDs das abas...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        // --- ETAPA 2: COLABORADORES ---
        console.log(">>> [ETAPA 2] Mapeando colaboradores...");
        const resColab = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { headers: gHeaders });
        const mapaNomes = {};
        if (resColab.data?.values) {
            resColab.data.values.forEach(r => { if (r[12]) mapaNomes[r[12]] = r[0]; });
        }

        // --- ETAPA 3: LOGIN HABLLA ---
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // JANELA DE 7 DIAS
        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        // --- ETAPA 4: LEITURA E LIMPEZA COM CRITÉRIO DE PARADA (20 LINHAS) ---
        console.log(`>>> [ETAPA 4] Analisando registros para limpeza de baixo para cima...`);
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        
        if (resSheet.data?.values) {
            const rows = resSheet.data.values;
            let blocosParaDeletar = [];
            let startIdx = -1;
            let contadorConsecutivasFora = 0;

            // Loop de baixo para cima (pula o cabeçalho no índice 0)
            for (let i = rows.length - 1; i >= 1; i--) {
                const dataRow = parseDataBR(rows[i][1]); // Analisa Coluna B

                if (dataRow && dataRow >= seteDiasAtras) {
                    // Linha está dentro do prazo: marcar para deletar e resetar contador
                    if (startIdx === -1) startIdx = i;
                    contadorConsecutivasFora = 0;
                } else {
                    // Linha fora do prazo ou vazia
                    contadorConsecutivasFora++;
                    
                    // Se tínhamos um bloco ativo, encerra ele aqui
                    if (startIdx !== -1) {
                        blocosParaDeletar.push({ start: i + 1, end: startIdx + 1 });
                        startIdx = -1;
                    }

                    // CRITÉRIO DE PARADA: 20 linhas seguidas fora do prazo
                    if (contadorConsecutivasFora >= 20) {
                        console.log(`[INFO] Parada atingida na linha ${i + 1} após 20 linhas consecutivas fora do prazo.`);
                        break;
                    }
                }
            }
            // Caso o loop termine ainda em um bloco (ex: chegou no cabeçalho)
            if (startIdx !== -1) blocosParaDeletar.push({ start: 1, end: startIdx + 1 });

            // --- ETAPA 5: EXECUÇÃO DA LIMPEZA ---
            if (blocosParaDeletar.length > 0) {
                console.log(`>>> [ETAPA 5] Removendo ${blocosParaDeletar.length} bloco(s) de linhas...`);
                // Ordenar para garantir que o Sheets processe corretamente (opcional em batchUpdate mas boa prática)
                const requests = blocosParaDeletar.map(b => ({
                    deleteDimension: { range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } }
                }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                console.log("Limpeza concluída.");
            } else {
                console.log(">>> [ETAPA 5] Nenhuma linha recente encontrada para apagar.");
            }
        }

        // --- ETAPA 6: BUSCA E INSERÇÃO (API HABLLA) ---
        console.log(">>> [ETAPA 6] Sincronizando novos dados da API...");
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
        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: new Date(ontem.setHours(0,0,0,0)).toISOString(), end_date: new Date(ontem.setHours(23,59,59,999)).toISOString() },
            headers: hHeaders
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
