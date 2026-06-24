/* === FINAL FIX V5: pindahkan hamburger ke card outlet, sebelah tombol edit === */
(function(){
  function moveKsMenuToOutlet(){
    try{
      const menuBtn = document.querySelector('.ks-menu-btn');
      const outlet = document.querySelector('.ks-outlet');
      if(!menuBtn || !outlet) return;

      let editBtn =
        outlet.querySelector('.ks-edit-btn') ||
        outlet.querySelector('.ks-outlet-edit') ||
        outlet.querySelector('button[onclick*="openOutlet"]') ||
        outlet.querySelector('button[onclick*="Setting"]') ||
        outlet.querySelector('button[onclick*="setting"]') ||
        outlet.querySelector('button:last-of-type');

      let actions = outlet.querySelector('.ks-outlet-actions');
      if(!actions){
        actions = document.createElement('div');
        actions.className = 'ks-outlet-actions';
        if(editBtn){
          outlet.insertBefore(actions, editBtn);
          actions.appendChild(menuBtn);
          actions.appendChild(editBtn);
        }else{
          outlet.appendChild(actions);
          actions.appendChild(menuBtn);
        }
      }else if(!actions.contains(menuBtn)){
        actions.insertBefore(menuBtn, actions.firstChild);
      }
    }catch(e){}
  }

  document.addEventListener('DOMContentLoaded', moveKsMenuToOutlet);
  setTimeout(moveKsMenuToOutlet, 80);
  setTimeout(moveKsMenuToOutlet, 350);
  setTimeout(moveKsMenuToOutlet, 1000);

  const oldGo = window.go;
  if(typeof oldGo === 'function'){
    window.go = function(){
      const r = oldGo.apply(this, arguments);
      setTimeout(moveKsMenuToOutlet, 80);
      return r;
    };
  }

  const oldRenderHome = window.renderHome;
  if(typeof oldRenderHome === 'function'){
    window.renderHome = function(){
      const r = oldRenderHome.apply(this, arguments);
      setTimeout(moveKsMenuToOutlet, 80);
      return r;
    };
  }
})();
