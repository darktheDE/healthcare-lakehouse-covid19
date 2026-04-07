# Development Guide

## 1. Setup & Chạy dự án

1. Clone repository về máy:
   ```bash
   git clone https://github.com/darktheDE/healthcare-lakehouse-covid19.git
   cd healthcare-lakehouse-covid19
   ```
2. Chuyển sang nhánh `develop`:
   ```bash
   git checkout develop
   ```
3. Cập nhật code mới nhất:
   ```bash
   git pull origin develop
   ```
4. Khởi chạy các dịch vụ (Docker):
   ```bash
   docker-compose -f deploy/docker-compose.yml up -d
   ```
5. Dừng các dịch vụ:
   ```bash
   docker-compose -f deploy/docker-compose.yml down
   ```

## 2. Quy trình phát triển (Git Workflow)

1. Cập nhật nhánh `develop`:
   ```bash
   git checkout develop
   git pull origin develop
   ```
2. Tạo nhánh `feature` từ `develop`:
   ```bash
   git checkout -b feature/<tên-tính-năng>
   ```
3. Code và commit:
   ```bash
   git add .
   git commit -m "feat: mô tả ngắn gọn công việc"
   ```
4. Push nhánh `feature` lên Github:
   ```bash
   git push origin feature/<tên-tính-năng>
   ```
5. Tạo Pull Request (PR):
   - Mở Github repository.
   - Tạo PR từ nhánh `feature/...` vào nhánh `develop`.
   - Chờ review và merge.

## 3. Quy tắc đặt tên

### Nhánh (Branch)
* `feature/...` - Phát triển tính năng mới
* `bugfix/...` - Sửa lỗi
* `chore/...` - Cập nhật thư viện, cấu hình

### Commit
* `feat:` - Tính năng mới
* `fix:` - Sửa lỗi
* `docs:` - Tài liệu
* `refactor:` - Cải thiện / tối ưu code
* `chore:` - Tooling, setup no production code change
