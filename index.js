const axios = require('axios');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. Sincroniza Colaboradores
        console.log(`[${new Date().toISOString()}] Sincronizando base de colaboradores...`);
        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => { if (row[12]) mapaNomes[row[12]] = row[0]; });
        }

        // 2. Login Hablla
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATAS ---
        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        const limiteCriacao = new Date();
        limiteCriacao.setDate(hoje.getDate() - 9); // Margem de segurança de 9 dias para o 'createdAt'

        const dISOInicio = seteDiasAtras.toISOString();

        // 3. LIMPEZA SELETIVA (Deleta apenas o que for >= 7 dias)
        console.log(`[${new Date().toISOString()}] Limpando registros recentes no Sheets...`);
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        if (resSheet.data?.values) {
            // Filtramos os índices das linhas que possuem data de criação (coluna B) dentro da nossa janela
            const indicesParaDeletar = resSheet.data.values
                .map((row, index) => {
                    const dataCriacaoStr = row[1]; // Coluna B
                    if (!dataCriacaoStr || index === 0) return -1;
                    const [d, m, y] = dataCriacaoStr.split(' ')[0].split('/');
                    const dataRow = new Date(`${y}-${m}-${d}T00:00:00Z`);
                    return dataRow >= seteDiasAtras ? index : -1;
                })
                .filter(i => i !== -1);

            if (indicesParaDeletar.length > 0) {
                // Deletamos de trás para frente para não corromper os índices durante a execução
                const requests = indicesParaDeletar.reverse().map(i => ({
                    deleteDimension: { range: { sheetId: 0, dimension: "ROWS", startIndex: i, endIndex: i + 1 } }
                }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                console.log(`[${new Date().toISOString()}] Limpeza de ${indicesParaDeletar.length} linhas concluída.`);
            }
        }

        // 4. BUSCA CARDS (Com trava de 2 páginas sem criação nova)
        let page = 1;
        let totalPages = 1;
        let paginasSemCriacaoNova = 0;
        let continuarBuscando = true;

        while (page <= totalPages && continuarBuscando) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { 
                    board: HABLLA_BOARD_ID, 
                    limit: 50, 
                    order: 'updated_at', // Mantém o foco no que foi alterado recentemente
                    page: page, 
                    updated_after: dISOInicio 
                },
                headers: hHeaders
            });

            const cards = res.data.results || [];
            totalPages = res.data.totalPages || 1;

            if (cards.length === 0) break;

            // LÓGICA DE TRAVA: Verifica se alguém na página foi CRIADO nos últimos 9 dias
            const temCriacaoNovaNestaPagina = cards.some(c => new Date(c.created_at) >= limiteCriacao);

            if (!temCriacaoNovaNestaPagina) {
                paginasSemCriacaoNova++;
                console.log(`[AVISO] Página ${page} sem cards criados nos últimos 9 dias. (${paginasSemCriacaoNova}/2)`);
            } else {
                paginasSemCriacaoNova = 0; // Reseta se achar um card realmente novo
            }

            if (paginasSemCriacaoNova >= 2) {
                console.log(`[STOP] Interrompendo: 2 páginas consecutivas sem novos protocolos criados.`);
                continuarBuscando = false;
                break;
            }

            console.log(`[${new Date().toISOString()}] Processando página ${page}...`);

            const rowsCards = cards.map(card => {
                const fmt = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR', {timeZone: 'UTC'}).replace(',', '') : "";
                let cf = ["", "", "", ""];
                const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                (card.custom_fields || []).forEach(f => {
                    const idx = ids.indexOf(f.custom_field);
                    if (idx !== -1) cf[idx] = f.value;
                });
                return [
                    fmt(card.updated_at), fmt(card.created_at), card.workspace, card.board, card.list,
                    cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                    card.user, fmt(card.finished_at), card.id, mapaNomes[card.user] || "", cf[3], (card.tags || []).map(t => t.name).join(", ")
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: gHeaders });
                await sleep(1200);
            }
            page++;
        }

        // 5. ATENDENTES (Sempre Ontem)
        console.log(`[${new Date().toISOString()}] Relatório de atendentes (Ontem)...`);
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const dISOOntemIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
        const dISOOntemFim = new Date(ontem.setHours(23,59,59,999)).toISOString();

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dISOOntemIni, end_date: dISOOntemFim },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ ontem.toLocaleDateString('pt-BR'), HABLLA_WORKSPACE_ID, s.id, s.name, u.id, mapaNomes[u.id] || "", u.email, item.total_services, item.tme, item.tma, c.id, c.name, c.type, item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, { values: rowsAt }, { headers: gHeaders });
        }
        console.log(`[${new Date().toISOString()}] Processamento concluído.`);

    } catch (e) {
        console.error("--- ERRO ---");
        if (e.response) console.error(JSON.stringify(e.response.data, null, 2));
        else console.error(e.message);
        process.exit(1);
    }
}
run();
