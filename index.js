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

        // --- LÓGICA DE DATAS (JANELA DE 7 DIAS) ---
        const hojeDataISO = new Date().toISOString().split('T')[0];
        let inicioBusca = new Date();
        
        if (hojeDataISO === "2026-03-19") {
            console.log("--- CARGA INICIAL COMPLETA (2026) ---");
            inicioBusca = new Date("2026-01-01T00:00:00Z");
        } else {
            console.log("--- EXECUÇÃO DIÁRIA: JANELA DE 7 DIAS ---");
            inicioBusca.setDate(inicioBusca.getDate() - 7);
            inicioBusca.setHours(0, 0, 0, 0);
        }
        const dISOInicio = inicioBusca.toISOString();

        // 3. LIMPEZA PREVENTIVA (SÓ SE NÃO FOR CARGA INICIAL)
        if (hojeDataISO !== "2026-03-19") {
            console.log(`[${new Date().toISOString()}] Limpando registros dos últimos 7 dias no Sheets...`);
            // Aqui usamos um Batch Update para filtrar e deletar. 
            // Para simplificar e ser seguro via API, vamos ler a aba e filtrar.
            const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R`, { headers: gHeaders });
            if (resSheet.data?.values) {
                const cabecalho = resSheet.data.values[0];
                const linhasMantidas = resSheet.data.values.filter((row, index) => {
                    if (index === 0) return true; // Mantém cabeçalho
                    const dataCriacao = row[1]; // Coluna B
                    if (!dataCriacao) return true;
                    // Converte "15/03/2026 10:00:00" para objeto Date para comparar
                    const [d, m, y] = dataCriacao.split(' ')[0].split('/');
                    const dataRow = new Date(`${y}-${m}-${d}T00:00:00Z`);
                    return dataRow < inicioBusca; // Mantém apenas o que for mais antigo que a janela de 7 dias
                });

                // Sobrescreve a planilha com os dados filtrados (limpando a área dos 7 dias)
                await axios.put(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A1?valueInputOption=USER_ENTERED`, 
                { values: linhasMantidas }, { headers: gHeaders });
                console.log(`[${new Date().toISOString()}] Limpeza concluída.`);
            }
        }

        // 4. BUSCA CARDS (HABLLA)
        let page = 1, totalPages = 1;
        while (page <= totalPages) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, order: 'updated_at', page: page, updated_after: dISOInicio },
                headers: hHeaders
            });

            totalPages = res.data.totalPages || 1;
            console.log(`[${new Date().toISOString()}] Processando página ${page} de ${totalPages}...`);

            const rowsCards = (res.data.results || []).map(card => {
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
                await sleep(1200); // Evita 429
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
