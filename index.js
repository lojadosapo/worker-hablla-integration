const axios = require('axios');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 
            'Authorization': `Bearer ${GOOGLE_TOKEN}`, 
            'Content-Type': 'application/json' 
        };

        // 1. BUSCAR O ID DA ABA DINAMICAMENTE (Evita o erro "No grid with id: 0")
        console.log(`[${new Date().toISOString()}] Obtendo metadados da planilha...`);
        const spreadsheetMeta = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, 
            { headers: gHeaders }
        );
        
        const findSheet = (name) => spreadsheetMeta.data.sheets.find(s => s.properties.title === name);
        
        const sheetHablla = findSheet("Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        // 2. Sincroniza Colaboradores (Base externa)
        console.log(`[${new Date().toISOString()}] Sincronizando base de colaboradores...`);
        const resDB = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, 
            { headers: gHeaders }
        );
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => { if (row[12]) mapaNomes[row[12]] = row[0]; });
        }

        // 3. Login Hablla
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { 
            email: HABLLA_EMAIL, 
            password: HABLLA_PASSWORD 
        });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATAS ---
        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0); // Início do dia há 7 dias

        const limiteBuscaAPI = new Date();
        limiteBuscaAPI.setDate(hoje.getDate() - 9); // Margem de 9 dias para a API por segurança

        console.log(`[INFO] Atualizando registros de ${seteDiasAtras.toLocaleDateString('pt-BR')} até hoje.`);

        // 4. LIMPEZA DOS ÚLTIMOS 7 DIAS NO SHEETS
        console.log(`[${new Date().toISOString()}] Lendo planilha para identificar linhas dos últimos 7 dias...`);
        const resSheet = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, 
            { headers: gHeaders }
        );

        if (resSheet.data?.values) {
            const indicesParaDeletar = resSheet.data.values
                .map((row, index) => {
                    const dataCriacaoStr = row[1]; // Coluna B (created_at)
                    if (!dataCriacaoStr || index === 0) return -1; // Pula cabeçalho ou vazio

                    // Formato esperado: "19/03/2026 09:00:44"
                    const [data] = dataCriacaoStr.split(' ');
                    const [d, m, y] = data.split('/');
                    const dataRow = new Date(`${y}-${m}-${d}T00:00:00Z`);

                    // Se a data for de 7 dias atrás para cá, marca para deletar
                    return dataRow >= seteDiasAtras ? index : -1;
                })
                .filter(i => i !== -1);

            if (indicesParaDeletar.length > 0) {
                console.log(`[${new Date().toISOString()}] Apagando ${indicesParaDeletar.length} linhas para sobrescrever...`);
                // Deleta de baixo para cima para não quebrar a ordem dos índices
                const requests = indicesParaDeletar.reverse().map(i => ({
                    deleteDimension: { 
                        range: { 
                            sheetId: idBaseHablla, 
                            dimension: "ROWS", 
                            startIndex: i, 
                            endIndex: i + 1 
                        } 
                    }
                }));
                await axios.post(
                    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, 
                    { requests }, 
                    { headers: gHeaders }
                );
            }
        }

        // 5. BUSCA CARDS NA API E INSERE O QUE FOI APAGADO
        let page = 1;
        let totalPages = 1;
        let paginasSemDados = 0;

        while (page <= totalPages) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { 
                    board: HABLLA_BOARD_ID, 
                    limit: 50, 
                    order: 'updated_at', 
                    page: page, 
                    updated_after: limiteBuscaAPI.toISOString() 
                },
                headers: hHeaders
            });

            const cards = res.data.results || [];
            totalPages = res.data.totalPages || 1;
            if (cards.length === 0) break;

            // Filtramos apenas os cards criados na nossa janela de 7 dias
            const rowsToInsert = cards
                .filter(card => new Date(card.created_at) >= seteDiasAtras)
                .map(card => {
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

            if (rowsToInsert.length > 0) {
                console.log(`[${new Date().toISOString()}] Inserindo ${rowsToInsert.length} cards atualizados (Pág ${page})...`);
                await axios.post(
                    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                    { values: rowsToInsert }, 
                    { headers: gHeaders }
                );
                await sleep(1500); // Evita Rate Limit do Google
                paginasSemDados = 0;
            } else {
                paginasSemDados++;
            }

            if (paginasSemDados >= 2) break; // Se não achar nada novo em 2 páginas, para
            page++;
        }

        // 6. RELATÓRIO DE ATENDENTES (Sempre Ontem)
        console.log(`[${new Date().toISOString()}] Gerando relatório de atendentes (Ontem)...`);
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const dISOIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
        const dISOFim = new Date(ontem.setHours(23,59,59,999)).toISOString();

        const resAt = await axios.get(
            `https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, 
            {
                params: { start_date: dISOIni, end_date: dISOFim },
                headers: hHeaders
            }
        );

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ 
                ontem.toLocaleDateString('pt-BR'), HABLLA_WORKSPACE_ID, s.id, s.name, u.id, 
                mapaNomes[u.id] || "", u.email, item.total_services, item.tme, item.tma, 
                c.id, c.name, c.type, item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr 
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(
                `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsAt }, 
                { headers: gHeaders }
            );
        }

        console.log(`[${new Date().toISOString()}] Sincronização concluída com sucesso!`);

    } catch (e) {
        console.error("--- ERRO NO PROCESSO ---");
        if (e.response) console.error(JSON.stringify(e.response.data, null, 2));
        else console.error(e.message);
        process.exit(1);
    }
}

run();
