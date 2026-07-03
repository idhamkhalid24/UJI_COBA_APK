    // ============================================================
    // Supabase
    // ============================================================
    const supabaseUrl = 'https://phuzgriglgovzcwgbehr.supabase.co';
    const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBodXpncmlnbGdvdnpjd2diZWhyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg1OTQ3ODUsImV4cCI6MjA5NDE3MDc4NX0.gf-cfo8Vos8uYgZRHG0KUB4xMhXAld2oTYv-wNiKneE';
    
    let supabaseClient;
    try {
        if (window.supabase) {
            supabaseClient = window.supabase.createClient(supabaseUrl, supabaseKey);
        } else {
            console.error('Library Supabase tidak ditemukan.');
            alert('Library Supabase gagal dimuat. Pastikan ada koneksi internet.');
        }
    } catch (e) {
        console.error('Error inisialisasi Supabase:', e);
        alert('Gagal inisialisasi Supabase: ' + e.message);
    }

    async function getAllTasks() {
        if (!supabaseClient) return [];
        try {
            const { data, error } = await supabaseClient.from('tasks').select('*');
            if (error) {
                console.error('Error fetching tasks:', error);
                return [];
            }
            return data || [];
        } catch (e) {
            console.error('Exception fetching tasks:', e);
            return [];
        }
    }

    async function putTask(task) {
        if (!supabaseClient) return;
        try {
            const { error } = await supabaseClient.from('tasks').upsert(task);
            if (error) {
                console.error('Error saving task:', error);
                alert('Gagal menyimpan tugas ke database. Cek console.');
            }
        } catch (e) {
            console.error('Exception saving task:', e);
        }
    }

    async function deleteTaskFromDB(taskId) {
        if (!supabaseClient) return;
        try {
            const { error } = await supabaseClient.from('tasks').delete().eq('id', taskId);
            if (error) {
                console.error('Error deleting task:', error);
            }
        } catch (e) {
            console.error('Exception deleting task:', e);
        }
    }

    async function clearAllTasksFromDB() {
        if (!supabaseClient) return;
        try {
            const { error } = await supabaseClient.from('tasks').delete().neq('id', '0');
            if (error) {
                console.error('Error clearing tasks:', error);
            }
        } catch (e) {
            console.error('Exception clearing tasks:', e);
        }
    }


    // ============================================================
    // i18n — Sistem Bahasa Indonesia / English
    // ============================================================
    const LANG = {
        id: {
            appName: 'Tugas Saya',
            sortBy: 'Urutkan',
            deadline: 'Tanggal',
            priority: 'Prioritas',
            searchPlaceholder: 'Cari tugas...',
            allTasks: 'Jadwal Hari Ini',
            active: 'Semua Tugas',
            sortDefault: 'Default',
            byCategory: 'Per Kategori',
            completed: 'Selesai',
            noTasks: 'Belum ada tugas',
            noTasksHint: 'Ketuk tombol + untuk mulai!',
            noCompletedTasks: 'Belum ada tugas yang selesai',
            noCompletedTasksHint: 'Tugas yang diselesaikan akan muncul di sini.',
            taskTitle: 'Judul Tugas*',
            briefDetails: 'Detail Singkat (Opsional)',
            briefPlaceholder: 'Tambahan info tentang tugas ini...',
            category: 'Kategori',
            categoryPlaceholder: 'cth. Kerja, Pribadi',
            timeLabel: 'Jam (Opsional)',
            useTime: ' Pakai Jam',
            addTask: 'Tambah Tugas',
            cancel: 'Batal',
            revisionNote: 'Catatan Revisi (Opsional)',
            revisionPlaceholder: 'Apa yang diubah dan kenapa?',
            saveChanges: 'Simpan',
            settings: 'Pengaturan',
            theme: 'Tema',
            language: 'Bahasa',
            export: 'Ekspor',
            exportBtn: 'Ekspor Semua (.txt)',
            data: 'Data',
            clearAll: 'Hapus Semua',
            storageNote: 'Data tersimpan lokal di perangkat ini.<br>Tidak perlu akun atau internet.',
            niceJob: 'Bagus! 🎉',
            offlineLabel: '100% Offline',
            high: 'Tinggi',
            medium: 'Sedang',
            low: 'Rendah',
            noDeadline: 'Tanpa tanggal',
            overdueLabel: '⚠ Terlambat',
            revisionHistoryLabel: 'Riwayat Revisi:',
            uncategorized: 'Tanpa Kategori',
            deleteConfirm: 'Hapus tugas ini?',
            clearConfirm: 'Hapus semua tugas? Ini tidak bisa dibatalkan.',
            noExport: 'Tidak ada tugas untuk diekspor.',
            noTitleAlert: 'Mohon isi judul tugas.',
            exportHeaderTitle: 'Daftar Tugas Saya',
            exportGenerated: 'Dibuat',
            exportTotal: 'Total',
            exportStatus: 'Status',
            exportDone: 'Selesai',
            exportPending: 'Belum selesai',
            exportCategory: 'Kategori',
            exportPriority: 'Prioritas',
            exportDeadline: 'Tanggal',
            exportDetails: 'Detail',
            exportRevisions: 'Revisi',
            exportOverdue: '(Terlambat)',
        },
        en: {
            appName: 'My Tasks',
            sortBy: 'Sort By',
            deadline: 'Deadline',
            priority: 'Priority',
            searchPlaceholder: 'Search tasks...',
            allTasks: 'Today\'s Schedule',
            active: 'All Tasks',
            sortDefault: 'Default',
            byCategory: 'By Category',
            completed: 'Completed',
            noTasks: 'No tasks found',
            noTasksHint: 'Tap the + button to get started!',
            noCompletedTasks: 'No completed tasks yet',
            noCompletedTasksHint: 'Tasks you complete will appear here.',
            taskTitle: 'Task Title*',
            briefDetails: 'Brief Details (Optional)',
            briefPlaceholder: 'Any additional details about this task...',
            category: 'Category',
            categoryPlaceholder: 'e.g., Work, Personal',
            timeLabel: 'Time (Optional)',
            useTime: ' Use Time',
            addTask: 'Add Task',
            cancel: 'Cancel',
            revisionNote: 'Revision Note (Optional)',
            revisionPlaceholder: 'What changes are you making and why?',
            saveChanges: 'Save Changes',
            settings: 'Settings',
            theme: 'Theme',
            language: 'Language',
            export: 'Export',
            exportBtn: 'Export All Tasks (.txt)',
            data: 'Data',
            clearAll: 'Clear All Tasks',
            storageNote: 'Data is stored locally on this device.<br>No account or internet needed.',
            niceJob: 'Nice job! 🎉',
            offlineLabel: '100% Offline',
            high: 'High',
            medium: 'Medium',
            low: 'Low',
            noDeadline: 'No deadline',
            overdueLabel: '⚠ Overdue',
            revisionHistoryLabel: 'Revision History:',
            uncategorized: 'Uncategorized',
            deleteConfirm: 'Delete this task?',
            clearConfirm: 'Clear all tasks? This cannot be undone.',
            noExport: 'No tasks to export.',
            noTitleAlert: 'Please enter a task title.',
            exportHeaderTitle: 'My Task List',
            exportGenerated: 'Generated',
            exportTotal: 'Total',
            exportStatus: 'Status',
            exportDone: 'Completed',
            exportPending: 'Pending',
            exportCategory: 'Category',
            exportPriority: 'Priority',
            exportDeadline: 'Deadline',
            exportDetails: 'Details',
            exportRevisions: 'Revisions',
            exportOverdue: '(Overdue)',
        }
    };

    let currentLang = localStorage.getItem('todo_lang') || 'id';

    function t(key) {
        return (LANG[currentLang] && LANG[currentLang][key]) || (LANG['en'][key]) || key;
    }

    function applyLang() {
        // Update all data-i18n elements
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            el.innerHTML = t(key);
        });
        // Update placeholders
        document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
            const key = el.getAttribute('data-i18n-placeholder');
            el.placeholder = t(key);
        });
        // Update lang toggle button
        const btn = document.getElementById('lang-toggle');
        if (btn) btn.textContent = currentLang === 'id' ? '🇬🇧 EN' : '🇮🇩 ID';
        // Update document title
        document.title = 'Dailish — ' + t('appName');
        // Update priority select options (they use value, not data-i18n target well enough for select)
        document.querySelectorAll('option[data-i18n]').forEach(opt => {
            opt.textContent = t(opt.getAttribute('data-i18n'));
        });
    }

    function switchLang() {
        currentLang = currentLang === 'id' ? 'en' : 'id';
        localStorage.setItem('todo_lang', currentLang);
        applyLang();
    }

    // ============================================================
    // App State
    // ============================================================
    let tasks = [];
    let allCategories = [];
    let currentEditingTask = null;
    let currentFilter = 'all';
    let currentSort = 'filter';
    let currentActiveSort = 'default';
    let currentSearch = '';

    // ============================================================
    // Utils
    // ============================================================
    function formatDate(deadlineStr) {
        if (!deadlineStr) return t('noDeadline');
        try {
            // deadlineStr bisa "2026-06-30" atau "2026-06-30T23:00"
            const hasTime = deadlineStr.includes('T') && deadlineStr.length > 10;
            const dt = hasTime ? new Date(deadlineStr) : new Date(deadlineStr + 'T00:00:00');
            const dateOpts = { weekday: 'short', month: 'short', day: 'numeric' };
            const datePart = dt.toLocaleDateString('en-US', dateOpts);
            if (hasTime) {
                const timePart = dt.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
                return `${datePart}, ${timePart}`;
            }
            return datePart;
        } catch { return deadlineStr; }
    }

    function isOverdue(deadlineStr) {
        if (!deadlineStr) return false;
        try {
            const hasTime = deadlineStr.includes('T') && deadlineStr.length > 10;
            const dt = hasTime ? new Date(deadlineStr) : new Date(deadlineStr + 'T23:59:59');
            return dt < new Date();
        } catch { return false; }
    }

    function updateCategories() {
        allCategories = [...new Set(tasks
            .filter(t => t.category && t.category.trim())
            .map(t => t.category.trim()))].sort();
    }

    // ============================================================
    // Render
    // ============================================================
    function renderRevisions(revisions) {
        if (!revisions || !revisions.length) return '';
        return `<div class="revision-history">
            <div style="font-weight:600;margin-bottom:0.5rem;">${t('revisionHistoryLabel')}</div>
            ${revisions.map(rev => `
                <div class="revision-item">
                    <div class="revision-date">${new Date(rev.date).toLocaleString()}</div>
                    <div class="revision-text">${rev.note}</div>
                </div>`).join('')}
        </div>`;
    }

    function renderTaskItem(task, taskList) {
        const li = document.createElement('li');
        li.className = `task-item${task.completed ? ' completed' : ''}`;
        li.dataset.id = task.id;
        const overdue = isOverdue(task.deadline) && !task.completed;
        li.innerHTML = `
            <div class="task-header">
                <input type="checkbox" class="task-checkbox"${task.completed ? ' checked' : ''}>
                <div class="task-title${task.completed ? ' completed' : ''}">${task.title}</div>
                <div class="task-actions">
                    <button class="pomodoro-btn" data-id="${task.id}" title="Mulai Fokus (Pomodoro)"><i class="fas fa-stopwatch"></i></button>
                    <span class="pomodoro-timer-display" id="pomodoro-display-${task.id}">25:00</span>
                    <button class="task-btn edit-btn" data-id="${task.id}"><i class="fas fa-edit"></i></button>
                    <button class="task-btn delete-btn" data-id="${task.id}"><i class="fas fa-trash"></i></button>
                </div>
            </div>
            <div class="task-meta">
                <div class="task-deadline${overdue ? ' overdue' : ''}">
                    <i class="fas fa-calendar-alt"></i> ${formatDate(task.deadline)}${overdue ? ' ' + t('overdueLabel') : ''}
                </div>
            </div>
            <div class="task-details">
                <div class="task-details-content">
                    ${renderRevisions(task.revisions)}
                </div>
            </div>`;
        taskList.appendChild(li);
    }

    function renderTasks() {
        const taskList = document.getElementById('task-list');
        const emptyState = document.getElementById('empty-state');
        
        const showEmptyState = () => {
            const p = emptyState.querySelector('p');
            const small = emptyState.querySelector('small');
            if (currentFilter === 'completed') {
                p.textContent = t('noCompletedTasks');
                small.textContent = t('noCompletedTasksHint');
            } else {
                p.textContent = t('noTasks');
                small.textContent = t('noTasksHint');
            }
            emptyState.style.display = 'block';
        };
        taskList.innerHTML = '';

        let filtered = [...tasks];

        if (currentSearch) {
            const q = currentSearch.toLowerCase();
            filtered = filtered.filter(t =>
                t.title.toLowerCase().includes(q) ||
                (t.brief && t.brief.toLowerCase().includes(q)) ||
                (t.category && t.category.toLowerCase().includes(q))
            );
        }

        const todayStr = new Date().toISOString().split('T')[0];
        if (currentFilter === 'all') {
            filtered = filtered.filter(t => t.deadline && t.deadline.startsWith(todayStr));
        } else if (currentFilter === 'active') {
            filtered = filtered.filter(t => !t.completed);
        } else if (currentFilter === 'completed') {
            filtered = filtered.filter(t => t.completed);
        }

        const sortFn = currentSort === 'deadline'
            ? (a, b) => new Date(a.deadline || '9999') - new Date(b.deadline || '9999')
            : currentSort === 'priority'
            ? (a, b) => ({ high:1, medium:2, low:3 }[a.priority] - { high:1, medium:2, low:3 }[b.priority])
            : (a, b) => new Date(b.createdAt) - new Date(a.createdAt);

        if (currentFilter === 'active' && currentActiveSort === 'category') {
            const byCategory = {};
            filtered.forEach(t => {
                const cat = t.category || t('uncategorized');
                if (!byCategory[cat]) byCategory[cat] = [];
                byCategory[cat].push(t);
            });
            if (!Object.keys(byCategory).length) { showEmptyState(); return; }
            emptyState.style.display = 'none';
            Object.keys(byCategory).sort().forEach(cat => {
                const h = document.createElement('h2');
                h.className = 'category-title';
                h.innerHTML = `<i class="fas fa-tag"></i> ${cat}`;
                taskList.appendChild(h);
                byCategory[cat].sort(sortFn).forEach(t => renderTaskItem(t, taskList));
            });
        } else {
            filtered.sort(sortFn);
            if (!filtered.length) { showEmptyState(); return; }
            emptyState.style.display = 'none';
            filtered.forEach(t => renderTaskItem(t, taskList));
        }

        attachTaskListeners();
    }

    function attachTaskListeners() {
        document.querySelectorAll('.edit-btn').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                const task = tasks.find(t => t.id === btn.dataset.id);
                if (task) openEditForm(task);
            };
        });
        document.querySelectorAll('.delete-btn').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                handleDelete(btn.dataset.id);
            };
        });
        document.querySelectorAll('.task-checkbox').forEach(cb => {
            cb.onchange = () => {
                const taskId = cb.closest('.task-item').dataset.id;
                handleToggleComplete(taskId);
            };
        });
        document.querySelectorAll('.pomodoro-btn').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                const taskId = btn.dataset.id;
                togglePomodoro(taskId, btn);
            };
        });
        document.querySelectorAll('.task-header').forEach(header => {
            header.onclick = (e) => {
                if (e.target.closest('button') || e.target.tagName === 'INPUT') return;
                header.closest('.task-item').classList.toggle('expanded');
            };
        });
    }

    // ============================================================
    // CRUD
    // ============================================================
    async function loadAndRender() {
        tasks = await getAllTasks();
        updateCategories();
        renderTasks();
    }

    async function handleAddTask() {
        const title = document.getElementById('task-title').value.trim();
        if (!title) { alert(t('noTitleAlert')); return; }

        const dateVal = document.getElementById('task-deadline').value;
        const hasTime = document.getElementById('task-has-time').checked;
        const timeVal = document.getElementById('task-time').value;
        const deadline = (hasTime && timeVal) ? `${dateVal}T${timeVal}` : dateVal;

        const newTask = {
            id: Date.now().toString(),
            title,
            brief: '',
            category: '',
            priority: 'medium',
            deadline,
            completed: false,
            createdAt: new Date().toISOString(),
            revisions: []
        };

        await putTask(newTask);
        animateAddTask();
        closeAddForm();
        await loadAndRender();
    }

    async function handleToggleComplete(taskId) {
        const task = tasks.find(t => t.id === taskId);
        if (!task) return;
        const updated = { ...task, completed: !task.completed };
        await putTask(updated);
        
        logTaskCompletion(updated.completed);
        
        if (updated.completed) {
            triggerConfetti();
            // showCelebration(); // using confetti now
        }
        await loadAndRender();
    }

    async function handleDelete(taskId) {
        if (!confirm(t('deleteConfirm'))) return;
        await deleteTaskFromDB(taskId);
        await loadAndRender();
    }

    async function handleSaveEdit() {
        if (!currentEditingTask) return;
        const title = document.getElementById('edit-task-title').value.trim();
        if (!title) { alert(t('noTitleAlert')); return; }

        const dateVal = document.getElementById('edit-task-deadline').value;
        const hasTime = document.getElementById('edit-has-time').checked;
        const timeVal = document.getElementById('edit-task-time').value;
        const deadline = (hasTime && timeVal) ? `${dateVal}T${timeVal}` : dateVal;

        const revisionNote = document.getElementById('edit-task-revision').value.trim();
        const updatedTask = {
            ...currentEditingTask,
            title,
            brief: currentEditingTask.brief || '',
            category: currentEditingTask.category || '',
            priority: currentEditingTask.priority || 'medium',
            deadline,
        };
        if (revisionNote) {
            updatedTask.revisions = [
                ...(currentEditingTask.revisions || []),
                { date: new Date().toISOString(), note: revisionNote }
            ];
        }

        await putTask(updatedTask);
        closeEditForm();
        await loadAndRender();
    }

    async function exportTXT() {
        const allTasks = await getAllTasks();
        if (!allTasks.length) { alert(t('noExport')); return; }

        let content = `My Task List\n\nGenerated: ${new Date().toLocaleString()}\nTotal: ${allTasks.length}\n\n${'─'.repeat(40)}\n\n`;
        allTasks.forEach((task, i) => {
            content += `${i+1}. ${task.title}\n`;
            content += `   ${t('exportStatus')}: ${task.completed ? t('exportDone') : t('exportPending')}\n`;
            content += `   ${t('exportCategory')}: ${task.category || t('uncategorized')}\n`;
            content += `   ${t('exportPriority')}: ${task.priority.toUpperCase()}\n`;
            content += `   ${t('exportDeadline')}: ${formatDate(task.deadline)}${isOverdue(task.deadline) && !task.completed ? ' ' + t('exportOverdue') : ''}\n`;
            if (task.brief) content += `   ${t('exportDetails')}: ${task.brief}\n`;
            if (task.revisions?.length) {
                content += `   ${t('exportRevisions')}:\n`;
                task.revisions.forEach((r, ri) => {
                    content += `     ${ri+1}. ${new Date(r.date).toLocaleString()}: ${r.note}\n`;
                });
            }
            content += `\n${'─'.repeat(40)}\n\n`;
        });

        const a = document.createElement('a');
        a.href = URL.createObjectURL(new Blob([content], { type: 'text/plain;charset=utf-8' }));
        a.download = `tasks_${new Date().toISOString().slice(0,10)}.txt`;
        a.click();
        URL.revokeObjectURL(a.href);
        toggleSettings(false);
    }

    // ============================================================
    // UI Helpers
    // ============================================================
    function openAddForm() {
        document.getElementById('edit-task-form').classList.remove('visible');
        document.getElementById('add-task-backdrop').classList.add('visible');
        document.getElementById('add-task-form').classList.add('visible');
        document.getElementById('fab').style.display = 'none';
        document.getElementById('task-title').focus();
        resetAddForm();
    }

    function closeAddForm() {
        document.getElementById('add-task-backdrop').classList.remove('visible');
        document.getElementById('add-task-form').classList.remove('visible');
        document.getElementById('fab').style.display = 'flex';
        resetAddForm();
    }

    function resetAddForm() {
        ['task-title'].forEach(id => {
            document.getElementById(id).value = '';
        });
        document.getElementById('task-deadline').value = new Date().toISOString().split('T')[0];
        const taskHasTime = document.getElementById('task-has-time');
        const taskTimeInput = document.getElementById('task-time');
        if (taskHasTime) taskHasTime.checked = false;
        if (taskTimeInput) { taskTimeInput.value = ''; taskTimeInput.disabled = true; taskTimeInput.style.opacity = '0.5'; }
        document.getElementById('category-suggestions').classList.remove('visible');
    }



    function openEditForm(task) {
        currentEditingTask = task;
        document.getElementById('add-task-form').classList.remove('visible');
        document.getElementById('add-task-backdrop').classList.add('visible');
        document.getElementById('edit-task-title').value = task.title;
        // Parse deadline
        const dl = task.deadline || '';
        const hasT = dl.includes('T') && dl.length > 10;
        document.getElementById('edit-task-deadline').value = hasT ? dl.split('T')[0] : dl;
        const editHasTime = document.getElementById('edit-has-time');
        const editTimeInput = document.getElementById('edit-task-time');
        if (hasT) {
            editHasTime.checked = true;
            editTimeInput.value = dl.split('T')[1];
            editTimeInput.disabled = false;
            editTimeInput.style.opacity = '1';
        } else {
            editHasTime.checked = false;
            editTimeInput.value = '';
            editTimeInput.disabled = true;
            editTimeInput.style.opacity = '0.5';
        }
        document.getElementById('edit-task-revision').value = '';
        document.getElementById('edit-task-form').classList.add('visible');
    }

    function closeEditForm() {
        document.getElementById('edit-task-form').classList.remove('visible');
        document.getElementById('add-task-backdrop').classList.remove('visible');
        currentEditingTask = null;
    }

    function toggleSettings(show) {
        document.getElementById('settings-panel').classList.toggle('visible', show);
    }



    function showCelebration() {
        const el = document.getElementById('celebration');
        const overlay = document.getElementById('celebration-overlay');
        el.classList.add('show');
        overlay.classList.add('show');
        setTimeout(() => { el.classList.remove('show'); overlay.classList.remove('show'); }, 2000);
    }

    function animateAddTask() {
        const btn = document.getElementById('add-task-btn');
        if (btn) { btn.style.animation = 'glow 1s ease'; setTimeout(() => btn.style.animation = '', 1000); }
    }

    function changeTheme(theme) {
        document.body.className = theme === 'light' ? '' : `${theme}-mode`;
        localStorage.setItem('todo_theme', theme);
        toggleSettings(false);
    }

    function showCategorySuggestions(inputEl, suggestionsEl, mode) {
        const val = inputEl.value.trim().toLowerCase();
        suggestionsEl.innerHTML = '';
        const matches = val
            ? allCategories.filter(c => c.toLowerCase().includes(val))
            : allCategories;
        if (!matches.length) { suggestionsEl.classList.remove('visible'); return; }
        matches.forEach(cat => {
            const div = document.createElement('div');
            div.className = 'category-suggestion';
            div.innerHTML = cat;
            div.addEventListener('mousedown', (e) => {
                e.preventDefault();
                inputEl.value = cat;
                suggestionsEl.classList.remove('visible');
            });
            suggestionsEl.appendChild(div);
        });
        suggestionsEl.classList.add('visible');
    }



    // ============================================================
    // NEW FEATURES (Habits, Pomodoro, Stats)
    // ============================================================
    
    // Confetti
    function triggerConfetti() {
        if (typeof confetti === 'function') {
            confetti({ particleCount: 100, spread: 70, origin: { y: 0.6 } });
        }
    }

    // Pomodoro
    let activePomodoros = {}; // { taskId: intervalId }
    function togglePomodoro(taskId, btnElement) {
        const display = document.getElementById(`pomodoro-display-${taskId}`);
        if (activePomodoros[taskId]) {
            // Stop
            clearInterval(activePomodoros[taskId]);
            delete activePomodoros[taskId];
            btnElement.classList.remove('active');
            display.innerText = "25:00";
        } else {
            // Start
            btnElement.classList.add('active');
            let timeLeft = 25 * 60; // 25 mins
            activePomodoros[taskId] = setInterval(() => {
                timeLeft--;
                const m = Math.floor(timeLeft / 60).toString().padStart(2, '0');
                const s = (timeLeft % 60).toString().padStart(2, '0');
                display.innerText = `${m}:${s}`;
                if (timeLeft <= 0) {
                    clearInterval(activePomodoros[taskId]);
                    delete activePomodoros[taskId];
                    btnElement.classList.remove('active');
                    display.innerText = "Selesai!";
                    triggerConfetti();
                    alert("Waktu Pomodoro selesai! Waktunya istirahat 5 menit.");
                }
            }, 1000);
        }
    }

    // Stats & Badges
    function logTaskCompletion(isCompleting) {
        const today = new Date().toISOString().split('T')[0];
        let stats = JSON.parse(localStorage.getItem('todo_stats') || '{}');
        if (isCompleting) {
            stats[today] = (stats[today] || 0) + 1;
        } else {
            stats[today] = Math.max(0, (stats[today] || 0) - 1);
        }
        localStorage.setItem('todo_stats', JSON.stringify(stats));
        
        const weeklyCount = checkBadges();
        if (document.getElementById('stats-modal').classList.contains('visible')) {
            document.getElementById('stats-weekly-count').innerText = weeklyCount;
        }
    }

    function checkBadges() {
        let stats = JSON.parse(localStorage.getItem('todo_stats') || '{}');
        let weeklyTotal = 0;
        const today = new Date();
        for(let i=0; i<7; i++) {
            const d = new Date(today);
            d.setDate(d.getDate() - i);
            const ds = d.toISOString().split('T')[0];
            weeklyTotal += (stats[ds] || 0);
        }
        if (weeklyTotal >= 50 && !localStorage.getItem('badge_50_awarded')) {
            localStorage.setItem('badge_50_awarded', 'true');
            const popup = document.getElementById('badge-popup');
            popup.classList.add('show');
            setTimeout(() => popup.classList.remove('show'), 5000);
        }
        return weeklyTotal;
    }

    // Habits
    let habits = JSON.parse(localStorage.getItem('todo_habits') || '[]');
    function saveHabits() { localStorage.setItem('todo_habits', JSON.stringify(habits)); }
    
    function checkHabitReset() {
        const today = new Date().toISOString().split('T')[0];
        const lastReset = localStorage.getItem('todo_habit_last_reset');
        if (lastReset !== today) {
            habits.forEach(h => h.completed = false);
            saveHabits();
            localStorage.setItem('todo_habit_last_reset', today);
        }
    }

    function renderHabits() {
        const container = document.getElementById('habit-section');
        const list = document.getElementById('habit-list');
        list.innerHTML = '';
        if (habits.length === 0) {
            container.style.display = 'none';
            return;
        }
        container.style.display = 'block';
        habits.forEach(h => {
            const li = document.createElement('li');
            li.className = `habit-item${h.completed ? ' completed' : ''}`;
            li.innerHTML = `
                <input type="checkbox" class="task-checkbox" ${h.completed ? 'checked' : ''}>
                <div style="flex:1; font-weight:600; font-size:var(--font-sm); ${h.completed ? 'text-decoration:line-through' : ''}">${h.name}</div>
                <button class="task-btn text-danger" onclick="deleteHabit('${h.id}')" style="color:#e63946;"><i class="fas fa-trash"></i></button>
            `;
            const cb = li.querySelector('input');
            cb.onchange = () => {
                h.completed = cb.checked;
                saveHabits();
                renderHabits();
                if(h.completed) triggerConfetti();
            };
            list.appendChild(li);
        });
    }

    window.deleteHabit = function(id) {
        habits = habits.filter(h => h.id !== id);
        saveHabits();
        renderHabits();
    };


    // ============================================================
    // Boot
    // ============================================================
    document.addEventListener('DOMContentLoaded', async () => {
        // Apply saved theme
        const savedTheme = localStorage.getItem('todo_theme');
        if (savedTheme) changeTheme(savedTheme);

        // Apply saved language
        applyLang();

        // Set default deadline
        document.getElementById('task-deadline').value = new Date().toISOString().split('T')[0];

        // Initialize Habits
        checkHabitReset();
        renderHabits();
        document.getElementById('add-habit-form').addEventListener('submit', (e) => {
            e.preventDefault();
            const input = document.getElementById('new-habit-input');
            const name = input.value.trim();
            if(name) {
                habits.push({ id: Date.now().toString(), name, completed: false });
                saveHabits();
                input.value = '';
                renderHabits();
            }
        });

        // Initialize Stats Modal
        document.getElementById('stats-btn').addEventListener('click', () => {
            const weeklyCount = checkBadges();
            document.getElementById('stats-weekly-count').innerText = weeklyCount;
            document.getElementById('stats-modal').classList.add('visible');
            document.getElementById('stats-backdrop').classList.add('visible');
        });
        document.getElementById('close-stats-btn').addEventListener('click', () => {
            document.getElementById('stats-modal').classList.remove('visible');
            document.getElementById('stats-backdrop').classList.remove('visible');
        });

        // Load tasks
        try {
            await loadAndRender();
        } catch (e) {
            console.error('Gagal memuat data dari Supabase:', e);
        }



        // Filter buttons
        document.getElementById('filter-all').addEventListener('click', async () => {
            currentFilter = 'all';
            document.getElementById('filter-all').classList.add('active');
            document.getElementById('filter-active').classList.remove('active');
            document.getElementById('filter-completed').classList.remove('active');
            renderTasks();
        });
        document.getElementById('filter-active').addEventListener('click', async () => {
            currentFilter = 'active';
            document.getElementById('filter-active').classList.add('active');
            document.getElementById('filter-all').classList.remove('active');
            document.getElementById('filter-completed').classList.remove('active');
            renderTasks();
        });
        document.getElementById('filter-completed').addEventListener('click', async () => {
            currentFilter = 'completed';
            document.getElementById('filter-completed').classList.add('active');
            document.getElementById('filter-all').classList.remove('active');
            document.getElementById('filter-active').classList.remove('active');
            renderTasks();
        });

        // Sort
        document.getElementById('sort-select').addEventListener('change', (e) => {
            currentSort = e.target.value;
            renderTasks();
        });


        // Search
        document.getElementById('search-btn').addEventListener('click', () => {
            const bar = document.getElementById('search-bar');
            bar.classList.toggle('visible');
            if (bar.classList.contains('visible')) {
                document.getElementById('search-input').focus();
            } else {
                document.getElementById('search-input').value = '';
                currentSearch = '';
                renderTasks();
            }
        });
        document.getElementById('search-input').addEventListener('input', (e) => {
            currentSearch = e.target.value;
            renderTasks();
        });

        // FAB & Add form
        document.getElementById('fab').addEventListener('click', openAddForm);
        document.getElementById('cancel-task-btn').addEventListener('click', closeAddForm);
        document.getElementById('add-task-form').addEventListener('click', (e) => e.stopPropagation());
        document.getElementById('add-task-backdrop').addEventListener('click', () => { closeAddForm(); closeEditForm(); });
        document.getElementById('add-task-btn').addEventListener('click', handleAddTask);
        document.getElementById('task-title').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') handleAddTask();
        });



        // Edit form
        document.getElementById('edit-task-form').addEventListener('click', (e) => e.stopPropagation());
        document.getElementById('save-edit-btn').addEventListener('click', handleSaveEdit);
        document.getElementById('cancel-edit-btn').addEventListener('click', closeEditForm);

        // Settings
        document.getElementById('hamburger-menu').addEventListener('click', () => toggleSettings(true));
        document.getElementById('close-settings').addEventListener('click', () => toggleSettings(false));
        document.getElementById('export-all-txt').addEventListener('click', exportTXT);
        document.getElementById('lang-toggle').addEventListener('click', () => { switchLang(); toggleSettings(false); });

        document.getElementById('clear-all-data').addEventListener('click', async () => {
            if (!confirm(t('clearConfirm'))) return;
            await clearAllTasksFromDB();
            await loadAndRender();
            toggleSettings(false);
        });

        document.querySelectorAll('.theme-option').forEach(opt => {
            opt.addEventListener('click', () => changeTheme(opt.dataset.theme));
        });

        // Checkbox "Pakai Jam" — Add form
        document.getElementById('task-has-time').addEventListener('change', (e) => {
            const ti = document.getElementById('task-time');
            ti.disabled = !e.target.checked;
            ti.style.opacity = e.target.checked ? '1' : '0.5';
            if (e.target.checked) ti.focus();
        });

        // Checkbox "Pakai Jam" — Edit form
        document.getElementById('edit-has-time').addEventListener('change', (e) => {
            const ti = document.getElementById('edit-task-time');
            ti.disabled = !e.target.checked;
            ti.style.opacity = e.target.checked ? '1' : '0.5';
            if (e.target.checked) ti.focus();
        });


    });
