CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    contrasena VARCHAR(255) NOT NULL,
    rol BOOLEAN DEFAULT FALSE,
    fecha_creacion TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    id_usuario INT REFERENCES usuarios(id),
    accion VARCHAR(100),
    descripcion TEXT,
    fecha_evento TIMESTAMP DEFAULT NOW(),
    nivel VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS pst_archivos (
    id SERIAL PRIMARY KEY,
    id_usuario INT REFERENCES usuarios(id),
    nombre_archivo VARCHAR(255),
    ruta_archivo VARCHAR(255),
    fecha_importacion TIMESTAMP DEFAULT NOW()
);
