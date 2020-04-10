Vue.component('nav-main', {
    props: ['active', ''],
    template: '<div>\
                <nav class="navbar bg-dark navbar-dark navbar-expand-sm fixed-top">\
                    <a class="navbar-brand" href="#">电影推荐系统</a>\
                    <div class="collapse navbar-collapse">\
                        <ul class="navbar-nav mr-auto">\
                            <li class="nav-item" :class="key==active?\'active\':\'\'" v-for="(value,key) in titles">\
                                <a class="nav-link" :href="key+\'.html\'">{{value}}</a>\
                            </li>\
                        </ul>\
                        <form class="form-inline">\
                            <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search">\
                            <button class="btn btn-outline-success my-2 my-sm-0" type="submit">Search</button>\
                        </form>\
                    </div>\
                </nav>\
                <div class="nav-partition"></div>\
               </div>',
    data: function () {
        return {
            titles: {
                'index': '主页',
                'genre': '分类',
                'movie': '电影',
                'recommend': '推荐',
            }
        }
    }
});