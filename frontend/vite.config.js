import vue from '@vitejs/plugin-vue';
import fs from 'fs';
import path from 'path';
import { defineConfig } from 'vite';

function mimeType(file) {
  const ext = path.extname(file).toLowerCase();
  const map = {
    '.html': 'text/html', '.htm': 'text/html', '.js': 'application/javascript', '.json': 'application/json',
    '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml',
    '.csv': 'text/csv', '.txt': 'text/plain'
  };
  return map[ext] || 'application/octet-stream';
}

// Serve /resources/* from repository root ../resources when running Vite dev server
export default defineConfig({
  plugins: [vue(), {
    name: 'serve-repo-resources',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        try {
          if (!req.url || !req.url.startsWith('/resources/')) return next();
          // map /resources/... to repo root ../resources/...
          // 解码 URL 以处理中文文件名
          const decodedUrl = decodeURIComponent(req.url);
          const rel = decodedUrl.replace(/^\/resources\//, '');
          const repoResources = path.resolve(__dirname, '..', 'resources', rel);
          console.log('[serve-resources]', req.url, '->', repoResources); // 调试日志
          if (!fs.existsSync(repoResources)) {
            console.log('[serve-resources] NOT FOUND:', repoResources);
            return next();
          }
          const stat = fs.statSync(repoResources);
          if (stat.isDirectory()) {
            // simple directory listing
            const entries = fs.readdirSync(repoResources, { withFileTypes: true });
            res.setHeader('content-type', 'text/html; charset=utf-8');
            res.write('<!doctype html><html><head><meta charset="utf-8"><title>Index of ' + req.url + '</title></head><body>');
            res.write('<h2>Index of ' + req.url + '</h2><ul>');
            // parent link
            let parent = path.posix.dirname(req.url);
            if (!parent.endsWith('/')) parent = parent + '/';
            if (parent && parent !== req.url) res.write('<li><a href="' + parent + '">../</a></li>');
            for (const e of entries) {
              const name = e.name + (e.isDirectory() ? '/' : '');
              const href = path.posix.join(req.url, name);
              res.write('<li><a href="' + href + '">' + name + '</a></li>');
            }
            res.end('</ul></body></html>');
            return;
          }
          // file
          const stream = fs.createReadStream(repoResources);
          res.setHeader('content-type', mimeType(repoResources));
          stream.pipe(res);
        } catch (err) {
          next();
        }
      });
    }
  }]
})
